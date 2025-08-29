import streamlit as st
import pandas as pd
from datetime import datetime
from . import db_utils
from . import prophet_model

def run():
    option_map = ["SSMS","POSTGRES"]
    
    selection = st.sidebar.pills(
        "DBMS",
        option_map,
        selection_mode="single",
        default="SSMS"
    )
    st.title("Aplikasi Prediksi Penjualan dengan Prophet")
    
    # UI elements for user inputs
    if selection == "SSMS":
        try:
            DB_NAME = st.secrets["ssms"]["dbname"]
        except KeyError:
            st.error("Kunci 'dbname' tidak ditemukan di st.secrets['ssms']. Harap periksa konfigurasi Anda.")
            return

        st.info(f"Menggunakan database: **{DB_NAME}**")

        # Branch
        raw_branch_list = db_utils.get_branch_list_ssms(DB_NAME)
        if raw_branch_list:
            branch_display_list = [f"{item[0]} | {item[1]}" for item in raw_branch_list]

            try:
                default_index_branch = [item[0] for item in raw_branch_list].index('BD32008')
            except ValueError:
                default_index_branch = 0

            selected_option = st.selectbox(
                "Pilih Branch yang Ingin Diprediksi (untuk prediksi tunggal):",
                branch_display_list,
                index=default_index_branch
            )
            branch_option = selected_option.split(' | ')[0]
            branch_name = selected_option.split(' | ')[1]
        else:
            st.warning("Tidak dapat memuat Branch dari database. Menggunakan input manual.")
            branch_option = st.text_input("Masukkan BranchId (misal: 'BD32008')", "BD32008")

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Tanggal Mulai Data Aktual:", pd.to_datetime("2022-01-01"))
        with col2:
            end_date = st.date_input("Tanggal Akhir Data Aktual:", pd.to_datetime("2025-07-31"))
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        kategori_options = db_utils.get_unique_categories_ssms(DB_NAME, start_date_str, end_date_str)
        if kategori_options:
            try:
                default_index = kategori_options.index('CIGARETTE')
            except ValueError:
                default_index = 0
            kategori_input = st.selectbox("Pilih Kategori yang Ingin Diprediksi (untuk prediksi tunggal):", kategori_options, index=default_index)
        else:
            kategori_input = st.text_input("Masukkan Kategori (misal: 'CIGARETTE')", "CIGARETTE")
            st.warning("Tidak dapat memuat kategori dari database. Menggunakan input manual.")

        periods_to_forecast = st.slider("Pilih jumlah hari untuk diprediksi:", 1, 365, 212)

        col3, col4 = st.columns(2)
        with col3:
            pisah_tanggal = st.date_input("Tanggal Mulai untuk Train Model", pd.to_datetime("2025-01-01")).strftime("%Y-%m-%d")
        with col4:
            st.markdown("Tampilkan visualisasi Komponen yang Dibaca Model")
            verbose = st.toggle("Aktifkan Visualisasi Komponen Model",label_visibility="hidden")

        st.write("---")
        st.header("Prediksi untuk Kategori Tunggal")
        if st.button("Jalankan Prediksi untuk Kategori Terpilih"):
            df = db_utils.load_data_ssms(DB_NAME, branch_option, kategori_input, start_date_str, end_date_str)
            if df.empty:
                st.warning("Tidak ada data yang dimuat. Pastikan koneksi dan kueri SQL benar.")
                # Tidak perlu 'return' jika ingin melanjutkan kode di bawah
            else:
                prophet_df, holidays_df, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates = prophet_model.prepare_data(df)
                if prophet_df is None:
                    # Tidak perlu 'return' jika ingin melanjutkan kode di bawah
                    pass
                else:
                    model, test_df, _, _, _ = prophet_model.train_and_evaluate(prophet_df, holidays_df, pisah_tanggal, verbose)
                    if model is None:
                        # Tidak perlu 'return' jika ingin melanjutkan kode di bawah
                        pass
                    else:
                        forecast_display_table = prophet_model.predict_table(model, periods_to_forecast, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates, pisah_tanggal, verbose)
                        prophet_model.display_charts(test_df, forecast_display_table)

        st.write("---")
        st.header("Prediksi Batch untuk Semua Kategori")
        if st.button("Mulai Prediksi Batch (Export ke Excel)"):
            prophet_model.batch_predict_and_export_all_categories(DB_NAME, branch_option, branch_name, kategori_input, start_date_str, end_date_str, periods_to_forecast, pisah_tanggal, selection)
            

    elif selection == "POSTGRES":
        try:
            DB_NAME = st.secrets["postgres"]["dbname"]
        except KeyError:
            st.error("Kunci 'dbname' tidak ditemukan di st.secrets['postgres']. Harap periksa konfigurasi Anda.")
            st.stop()
        st.info(f"Menggunakan database: **{DB_NAME}**")

        # Branch
        branch_list = db_utils.get_branch_list_postgres()
        if branch_list:
            branch_display_list = [item[0] for item in branch_list]

            try:
                default_index_branch = [item[0] for item in branch_list].index('BD32007')
            except ValueError:
                default_index_branch = 0

            selected_option = st.selectbox(
                "Pilih Branch yang Ingin Diprediksi (untuk prediksi tunggal):",
                branch_display_list,
                index=default_index_branch
            )
            branch_option = branch_list[branch_display_list.index(selected_option)][0]
            branch_name = "Manual Input"
        else:
            st.warning("Tidak dapat memuat Branch dari database. Menggunakan input manual.")
            branch_option = st.text_input("Masukkan BranchId (misal: 'BD32007')", "BD32007")

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Tanggal Mulai Data Aktual:", pd.to_datetime("2022-01-01"))
        with col2:
            end_date = st.date_input("Tanggal Akhir Data Aktual:", pd.to_datetime("2025-07-31"))
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        kategori_options = db_utils.get_unique_categories_postgres(start_date_str, end_date_str)
        if kategori_options:
            try:
                default_index = kategori_options.index('CIGARETTE')
            except ValueError:
                default_index = 0
            kategori_input = st.selectbox("Pilih Kategori yang Ingin Diprediksi (untuk prediksi tunggal):", kategori_options, index=default_index)
        else:
            kategori_input = st.text_input("Masukkan Kategori (misal: 'CIGARETTE')", "CIGARETTE")
            st.warning("Tidak dapat memuat kategori dari database. Menggunakan input manual.")

        periods_to_forecast = st.slider("Pilih jumlah hari untuk diprediksi:", 1, 365, 212)

        col3, col4 = st.columns(2)
        with col3:
            pisah_tanggal = st.date_input("Tanggal Mulai untuk Train Model", pd.to_datetime("2025-01-01")).strftime("%Y-%m-%d")
        with col4:
            st.markdown("Tampilkan visualisasi Komponen yang Dibaca Model")
            verbose = st.toggle("Aktifkan Visualisasi Komponen Model",label_visibility="hidden")

        st.write("---")
        st.header("Prediksi untuk Kategori Tunggal")
        if st.button("Jalankan Prediksi untuk Kategori Terpilih"):
            # Panggilan fungsi tanpa parameter DB_NAME
            df = db_utils.load_data_postgres(branch_option, kategori_input, start_date_str, end_date_str)
            if df.empty:
                st.warning("Tidak ada data yang dimuat. Pastikan koneksi dan kueri SQL benar.")
                return
            
            prophet_df, holidays_df, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates = prophet_model.prepare_data(df)
            if prophet_df is None: return

            model, test_df, _, _, _ = prophet_model.train_and_evaluate(prophet_df, holidays_df, pisah_tanggal,verbose)
            if model is None: return

            forecast_display_table = prophet_model.predict_table(model, periods_to_forecast, all_holiday_dates_set, all_ramadan_dates, all_ujian_dates, pisah_tanggal,verbose)
            prophet_model.display_charts(test_df, forecast_display_table)

        st.write("---")
        st.header("Prediksi Batch untuk Semua Kategori")
        if st.button("Mulai Prediksi Batch (Export ke Excel)"):
            # Panggilan fungsi tanpa parameter DB_NAME, menambahkan branch_name
            prophet_model.batch_predict_and_export_all_categories(DB_NAME, branch_option, branch_name, kategori_input, start_date_str, end_date_str, periods_to_forecast, pisah_tanggal, selection)
    else:
        st.warning("Harap pilih salah satu pillbox di sidebar")