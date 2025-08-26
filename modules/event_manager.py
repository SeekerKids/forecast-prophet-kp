import streamlit as st
import time
import pandas as pd
from datetime import datetime
from . import event_utils
import os # Import the os module

def run():
    # current_year = datetime.now().year
    start_year = st.sidebar.number_input("Tahun Awal",  value=2015)
    end_year = st.sidebar.number_input("Tahun Akhir", min_value=start_year, value=2025)
    
    if start_year > end_year:
        st.sidebar.error("Rentang tahun tidak valid.")
        return False
    
    st.title("Aplikasi Pengelola Data Hari Libur & Event")
    
    # Check if the file exists before creating it
    if not os.path.exists(event_utils.EVENTS_EXCEL_FILE):
        event_utils.create_events_excel_file(start_year, end_year)
    
    if st.button("Lakukan Scraping & Perbarui Data"):
        with st.spinner('Sedang melakukan scraping...'):
            event_utils.update_holidays_data(start_year, end_year)
            time.sleep(3)
            st.rerun()

    sheet_options = ['Holidays', 'Ramadan', 'Ujian']
    selected_sheet = st.selectbox("Pilih Sheet", sheet_options)
    st.header(f"Data {selected_sheet}")
    
    # Load data from the existing file
    df = event_utils.load_data(selected_sheet)
    edited_df = df.copy()

    if not df.empty:
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True
        )
        if st.button("Simpan Perubahan"):
            if event_utils.save_changes(edited_df, selected_sheet, sheet_options):
                time.sleep(2)
                st.rerun()

    st.markdown("---")
    st.header(f"Tambah Data Baru ke Sheet {selected_sheet}")
    if selected_sheet == 'Holidays':
        unique_names = sorted(edited_df['Holiday Name'].unique().tolist())
        col1, col2 = st.columns(2)

        with col1:
            new_date = st.date_input("Pilih Tanggal")
        with col2:
            all_options = unique_names + ['Buat Baru']
            selected_option = st.selectbox("Pilih Keterangan", all_options, index=None)

        new_description = None
        if selected_option == 'Buat Baru':
            new_description = st.text_input("Keterangan Baru")
        elif selected_option:
            new_description = selected_option

        if st.button("Tambah Data Holiday"):
            updated_df = event_utils.add_holiday_form(edited_df, new_date, new_description)
            if updated_df is not None:
                st.success("Data berhasil ditambahkan.")
                st.dataframe(updated_df, use_container_width=True)
                time.sleep(2)
                st.rerun()

    elif selected_sheet in ['Ramadan', 'Ujian']:
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Tanggal Awal")
        with col2:
            end_date = st.date_input("Tanggal Akhir")

        # Tombol aksi
        if st.button("Tambah Data Periode"):
            if event_utils.validate_dates(start_date, end_date):
                updated_df = event_utils.add_period(edited_df, start_date, end_date,selected_sheet)
                if updated_df is not None:
                    st.success("Data berhasil ditambahkan.")
                    st.dataframe(updated_df, use_container_width=True)
                    time.sleep(2)
                    st.rerun()
            else:
                st.warning("Mohon isi tanggal awal dan akhir dengan benar.")


