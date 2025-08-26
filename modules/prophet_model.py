import streamlit as st
import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_squared_error, r2_score
from math import sqrt
import warnings
# from modules import EVENTS_EXCEL_FILE
from . import event_utils
from . import db_utils
import os

warnings.filterwarnings("ignore")
@st.cache_data
def prepare_events_prophet(file):
    '''I.S. AMBIL EVENT DARI FILE (EXCEL)
        O.S DATAFRAME EVENT DENGAN LOWER & UPPER WINDOW
    '''
    try:
        holidays_df = pd.read_excel(file, sheet_name='Holidays')
        holidays_df = holidays_df.rename(columns={"Date": "ds", "Holiday Name": "holiday"})
        holidays_df["ds"] = pd.to_datetime(holidays_df["ds"])
        all_holiday_dates_set = set(holidays_df['ds'].dt.date)
    except Exception as e:
        st.error(f"Gagal memuat sheet 'Holidays': {e}")
        holidays_df = pd.DataFrame(columns=["ds", "holiday"])
        all_holiday_dates_set = set()
    all_ramadan_dates = set()
    try:
        ramadan_df = pd.read_excel(file, sheet_name='Ramadan')
        ramadan_df['Start Date'] = pd.to_datetime(ramadan_df['Start Date'])
        ramadan_df['End Date'] = pd.to_datetime(ramadan_df['End Date'])
        for _, row in ramadan_df.iterrows():
            if pd.notna(row['Start Date']) and pd.notna(row['End Date']):
                current_ramadan_range = pd.date_range(start=row['Start Date'], end=row['End Date'], freq='D')
                all_ramadan_dates.update(d.date() for d in current_ramadan_range)
    except Exception as e: st.error(f"Gagal memuat sheet 'Ramadan': {e}")
    all_ujian_dates = set()
    try:
        ujian_df = pd.read_excel(file, sheet_name='Ujian')
        ujian_df['Start Date'] = pd.to_datetime(ujian_df['Start Date'])
        ujian_df['End Date'] = pd.to_datetime(ujian_df['End Date'])
        for _, row in ujian_df.iterrows():
            if pd.notna(row['Start Date']) and pd.notna(row['End Date']):
                current_ujian_range = pd.date_range(start=row['Start Date'], end=row['End Date'], freq='D')
                all_ujian_dates.update(d.date() for d in current_ujian_range)
    except Exception as e: st.error(f"Gagal memuat sheet 'Ujian': {e}")
    holidays_df['lower_window'] = 0
    holidays_df['upper_window'] = 0
    return holidays_df, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates

@st.cache_data
def prepare_data(df):
    '''
    FEATURE ENGINEERING UNTUK MODEL PROPHET
    '''
    holidays_df, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates = prepare_events_prophet(event_utils.EVENTS_EXCEL_FILE)
    df['IsHoliday'] = df.index.map(lambda x: 1 if x.date() in all_holiday_dates_set else 0)
    df['IsRamadan'] = df.index.map(lambda x: 1 if x.date() in all_ramadan_dates else 0)
    df['IsUjian'] = df.index.map(lambda x: 1 if x.date() in all_ujian_dates else 0)
    df['day_of_week'] = df.index.dayofweek
    df['month'] = df.index.month
    df['year'] = df.index.year
    df['weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['libur'] = df['IsHoliday'] | df['weekend']
    df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce')
    df.dropna(subset=['Sales'], inplace=True)
    if not df['Sales'].empty:
        df['y'] = df['Sales'].apply(lambda x: np.clip(x, df['Sales'].quantile(0.01), df['Sales'].quantile(0.99)))
    else:
        st.warning("Kolom 'Sales' kosong setelah pembersihan data. Tidak dapat melanjutkan.")
        return None, None, None, None, None
    prophet_df = df.reset_index().rename(columns={'Date': 'ds', 'y': 'y'})
    prophet_df = prophet_df[['ds', 'y', 'IsHoliday', 'IsRamadan', 'IsUjian', 'day_of_week', 'month', 'year', 'weekend', 'libur']]
    return prophet_df, holidays_df, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates

def train_and_evaluate(prophet_df, holidays_df, pisah_tanggal, verbose=True):
    split_date = pd.to_datetime(pisah_tanggal)
    train_df = prophet_df[prophet_df['ds'] < split_date]
    test_df = prophet_df[prophet_df['ds'] >= split_date]
    if train_df.empty:
        if verbose: st.error("Data pelatihan kosong. Pastikan rentang tanggal data mencakup periode sebelum tanggal batas.")
        return None, None, None, None, None
    model = Prophet(holidays=holidays_df, yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=False)
    for regressor in ['IsRamadan', 'IsUjian', 'day_of_week', 'month', 'year', 'weekend', 'libur']:
        model.add_regressor(regressor)
    model.fit(train_df)
    rmse, r2, mape = None, None, None
    if verbose: st.subheader("Evaluasi Model")
    if not test_df.empty:
        future_test = test_df[['ds', 'IsHoliday', 'IsRamadan', 'IsUjian', 'day_of_week', 'month', 'year', 'weekend', 'libur']].copy()
        forecast_test = model.predict(future_test)
        test_with_forecast = pd.merge(test_df, forecast_test, on='ds')
        y_true = test_with_forecast['y']
        y_pred = test_with_forecast['yhat']
        rmse = sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)
        mape = np.mean(np.abs((y_true - y_pred) / y_true).replace([np.inf, -np.inf], np.nan).dropna()) * 100 
        if verbose:
            st.success("✅ Evaluasi berhasil!")
            st.metric("Root Mean Squared Error (RMSE)", f"{rmse:.2f}")
            st.metric("R-squared (R²)", f"{r2:.2f}")
            st.metric("Mean Absolute Percentage Error (MAPE)", f"{mape:.2f}%")
            st.write("---")
            st.subheader("Visualisasi Prediksi pada Data Uji")
            fig_eval = model.plot(forecast_test)
            st.pyplot(fig_eval)
    else:
        if verbose: st.warning("Data pengujian kosong. Tidak dapat melakukan evaluasi.")
    return model, test_df, rmse, r2, mape

def predict_and_display(model, periods_to_forecast, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates, pisah_tanggal,verbose=True):
    if verbose: st.subheader("Prediksi Masa Depan")
    with st.spinner(f"Membuat prediksi untuk {periods_to_forecast} hari..."):
        future = model.make_future_dataframe(periods=periods_to_forecast, freq='D', include_history=True)
        future['IsHoliday'] = future['ds'].map(lambda x: 1 if x.date() in all_holiday_dates_set else 0).astype(int)
        future['IsRamadan'] = future['ds'].map(lambda x: 1 if x.date() in all_ramadan_dates else 0).astype(int)
        future['IsUjian'] = future['ds'].map(lambda x: 1 if x.date() in all_ujian_dates else 0).astype(int)
        future['day_of_week'] = future['ds'].dt.dayofweek
        future['month'] = future['ds'].dt.month
        future['year'] = future['ds'].dt.year
        future['weekend'] = future['day_of_week'].isin([5, 6]).astype(int)
        future['libur'] = future['IsHoliday'] | future['weekend']
        future.replace([np.inf, -np.inf], np.nan, inplace=True)
        forecast = model.predict(future)
        if verbose:
            fig1 = model.plot(forecast) 
            st.pyplot(fig1)
            st.subheader("Komponen Prediksi")
            fig2 = model.plot_components(forecast)
            st.pyplot(fig2)
            st.subheader("Tabel Prediksi")
            
        forecast_display_table = forecast[forecast['ds'] >= pisah_tanggal].copy()
        forecast_display_table = forecast_display_table[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        for col in ['yhat', 'yhat_lower', 'yhat_upper']:
            forecast_display_table[col] = forecast_display_table[col].apply(lambda x: int(round(x)) if pd.notna(x) else np.nan)
        if verbose: st.dataframe(forecast_display_table)
    return forecast_display_table

def display_charts(test_df, forecast_display_table):
    st.subheader("Perbandingan Aktual vs. Prediksi")
    if test_df is not None and not test_df.empty:
        # Perbandingan Actual vs. Test
        st.write("### Actual vs. Test")
        actual_sales_test = test_df.set_index('ds')['y']
        predicted_sales_test_period = forecast_display_table[
            forecast_display_table['ds'].isin(actual_sales_test.index.to_numpy())
        ].set_index('ds')['yhat']
        combined_df = pd.concat([actual_sales_test, predicted_sales_test_period], axis=1)
        combined_df.columns = ['Penjualan Aktual', 'Penjualan Prediksi']
        st.line_chart(combined_df, color=["#FF0000", "#0000FF"])
        
        # Perbandingan Actual vs. Forecast
        st.write("### Actual vs. Forecast (Gabungan)")
        forecast_display_table['ds'] = pd.to_datetime(forecast_display_table['ds'])
        test_df['ds'] = pd.to_datetime(test_df['ds'])
        df_merged = pd.merge(forecast_display_table, test_df, on='ds', how='left')
        df_filtered = df_merged[df_merged['ds'] >= test_df['ds'].min()]
        df_filtered.set_index('ds', inplace=True)
        chart_df = df_filtered[['y', 'yhat']].rename(columns={'y': 'Penjualan Aktual', 'yhat': 'Penjualan Prediksi'})
        st.line_chart(chart_df, color=["#FF0000", "#0000FF"])

    st.subheader("Prediksi")
    forecast_display_table['ds'] = pd.to_datetime(forecast_display_table['ds'])
    forecast_display_table.set_index('ds', inplace=True)
    chart_df = forecast_display_table[['yhat']].rename(columns={'yhat': 'Penjualan Prediksi'})
    st.line_chart(chart_df, color=["#0000FF"])


def batch_predict_and_export_all_categories(db_name, start_date_str, end_date_str, periods_to_forecast,pisah_tanggal):
    """
    Melakukan prediksi batch untuk semua kategori dan mengekspor hasilnya ke file Excel.
    """
    st.subheader("Proses Prediksi Batch")
    
    # Create directory for database if it doesn't exist
    output_folder = os.path.join(os.getcwd(), db_name)
    os.makedirs(output_folder, exist_ok=True)
    st.write(f"Menyimpan file ke folder: `{output_folder}`")

    all_categories = db_utils.get_unique_categories(db_name, start_date_str, end_date_str)
    
    if not all_categories:
        st.error(f"Tidak ada kategori unik yang ditemukan dalam database '{db_name}' untuk rentang tanggal yang diberikan. Tidak dapat melakukan prediksi batch.")
        return

    progress_bar = st.progress(0)
    status_text = st.empty()
    total_categories = len(all_categories)

    for i, category in enumerate(all_categories):
        status_text.text(f"Memproses kategori: {category} ({i+1}/{total_categories})")
        progress_bar.progress((i + 1) / total_categories)

        try:
            # 1. Load Data
            # df = prepare_data(db_name, category, start_date_str, end_date_str)
            df = db_utils.load_data(db_name, category, start_date_str, end_date_str)

            if df.empty:
                st.warning(f"Tidak ada data untuk kategori '{category}'. Melanjutkan ke kategori berikutnya.")
                continue
            

            # 2. Prepare Prophet Data
            prophet_df, holidays_df, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates = prepare_data(df)
            if prophet_df is None: # Check if data preparation failed
                st.warning(f"Gagal mempersiapkan data untuk kategori '{category}'. Melanjutkan ke kategori berikutnya.")
                continue

            # 3. Train and Evaluate Prophet Model (silent mode for batch)
            model, _, rmse, r2, mape = train_and_evaluate(prophet_df, holidays_df,pisah_tanggal, verbose=False)

            if model is None:
                st.warning(f"Gagal melatih model untuk kategori '{category}'. Melanjutkan ke kategori berikutnya.")
                continue

            # 4. Generate Future Predictions (get the full forecast)
            forecast_full = predict_and_display(model, periods_to_forecast, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates,pisah_tanggal,verbose=False)
            # Prepare data for Excel export: filter for 2025 onwards, subset, and integer conversion
            forecast_for_excel = (
                forecast_full[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
                .copy()
                .reset_index(drop=True)
                .rename(columns={
                    'ds': 'Tanggal',
                    'yhat': 'Prediksi',
                    'yhat_lower': 'Batas Bawah',
                    'yhat_upper': 'Batas Atas'
                }) 
            )     
            for col in ['Prediksi', 'Batas Bawah', 'Batas Atas']:
                forecast_for_excel[col] = forecast_for_excel[col].apply(lambda x: int(round(x)) if pd.notna(x) else np.nan)

            if forecast_for_excel.empty:
                st.warning(f"Tidak ada prediksi untuk tahun 2025 atau lebih untuk kategori '{category}'. Melewatkan ekspor.")
                continue

            # Get prediction start and end dates for filename from the filtered forecast
            pred_start_date = forecast_for_excel['Tanggal'].min().strftime("%Y%m%d")
            pred_end_date = forecast_for_excel['Tanggal'].max().strftime("%Y%m%d")

            # Handle cases where R2 or MAPE might be None
            r2_str = f"{r2:.2f}" if r2 is not None else "N/A"
            mape_str = f"{mape:.2f}" if mape is not None else "N/A"

            # Construct filename
            filename = f"{category}_{pred_start_date}_{pred_end_date}_R2 = {r2_str}_MAPE = {mape_str}.xlsx"
            file_path = os.path.join(output_folder, filename)

            # Save to Excel
            forecast_for_excel.to_excel(file_path, index=False)
            st.success(f"✅ Berhasil menyimpan '{filename}'")

        except Exception as e:
            st.error(f"❌ Error saat memproses kategori '{category}': {e}")
            
    status_text.empty()
    st.success("🎉 Prediksi Batch selesai untuk semua kategori!")
