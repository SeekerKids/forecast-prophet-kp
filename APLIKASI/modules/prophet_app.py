import streamlit as st
import pandas as pd
from datetime import datetime
from . import db_utils
from . import prophet_model

def run():
    st.title("Aplikasi Prediksi Penjualan dengan Prophet")
    
    # UI elements for user inputs
    server_conn = db_utils.get_server_connection()
    database_list = db_utils.get_database_list(server_conn) if server_conn else []
    DB_NAME = st.selectbox("Pilih nama database:", database_list)
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Tanggal Mulai Data:", pd.to_datetime("2022-01-01"))
    with col2:
        end_date = st.date_input("Tanggal Akhir Data:", pd.to_datetime("2025-07-31"))
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    kategori_options = db_utils.get_unique_categories(DB_NAME, start_date_str, end_date_str)
    if kategori_options:
        try:
            default_index = kategori_options.index('CIGARETTE')
        except ValueError:
            default_index = 0
        kategori_input = st.selectbox("Pilih Kategori (untuk prediksi tunggal):", kategori_options, index=default_index)
    else:
        kategori_input = st.text_input("Masukkan Kategori (misal: 'CIGARETTE')", "CIGARETTE")
        st.warning("Tidak dapat memuat kategori dari database. Menggunakan input manual.")

    periods_to_forecast = st.slider("Pilih jumlah hari untuk diprediksi:", 1, 365, 212)
    pisah_tanggal = st.date_input("Tanggal Batas Train Data", pd.to_datetime("2025-01-01")).strftime("%Y-%m-%d")
    
    st.write("---")
    st.header("Prediksi untuk Kategori Tunggal")
    if st.button("Jalankan Prediksi untuk Kategori Terpilih"):
        df = db_utils.load_data(DB_NAME, kategori_input, start_date_str, end_date_str)
        if df.empty:
            st.warning("Tidak ada data yang dimuat. Pastikan koneksi dan kueri SQL benar.")
            return

        prophet_df, holidays_df, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates = prophet_model.prepare_data(df)
        if prophet_df is None: return

        model, test_df, _, _, _ = prophet_model.train_and_evaluate(prophet_df, holidays_df, pisah_tanggal)
        if model is None: return

        forecast_display_table = prophet_model.predict_and_display(model, periods_to_forecast, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates, pisah_tanggal)
        prophet_model.display_charts(test_df, forecast_display_table)
        
    st.write("---")
    st.header("Prediksi Batch untuk Semua Kategori")
    if st.button("Mulai Prediksi Batch (Export ke Excel)"):
        prophet_model.batch_predict_and_export_all_categories(DB_NAME, start_date_str, end_date_str, periods_to_forecast,pisah_tanggal)