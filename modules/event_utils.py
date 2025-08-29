import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import io
from datetime import datetime, timedelta
import streamlit as st

EVENTS_EXCEL_FILE = "events.xlsx"
BASE_URL = "https://tanggalan.com/"
MONTHS = ["januari", "februari", "maret", "april", "mei", "juni", "juli", "agustus", "september", "oktober", "november", "desember"]

def last_update_excel():
    if os.path.exists(EVENTS_EXCEL_FILE):
        last_modified_time = os.path.getmtime(EVENTS_EXCEL_FILE)
        last_modified_dt = datetime.fromtimestamp(last_modified_time)
        st.markdown(f"**Terakhir diperbarui:** {last_modified_dt.strftime('%d %B %Y %H:%M:%S')}")
    else:
        st.warning("File events.xlsx tidak ditemukan. File akan dibuat saat pertama kali dijalankan.")


def parse_holiday_range(range_str):
    '''
        PEMISAH RANGE SCRAPPING HARI LIBUR (15-17 CUTI BERSAMA IDUL FITRI)
    '''

    range_str = range_str.strip()
    if not range_str: return []
    if '-' in range_str:
        parts = range_str.split('-')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            start, end = map(int, parts)
            return list(range(start, end + 1))
        else: return []
    if range_str.isdigit(): return [int(range_str)]
    return []
    
def clean_keterangan(desc):
    '''
        MEMBERSIHKAN KETERANGAN HARI LIBUR AGAR SERAGAM
    '''

    desc = ''.join([c for c in desc if not c.isdigit()]).strip()
    desc = desc.replace("Raya Waisak","Waisak").replace("Isra'","Isra").replace("Suci","Raya").replace("  H", "").replace("  Muharram H", "").replace("-"," ").replace("ha H", "ha").replace(" Tahun Baru Saka", "").replace("ri H", "ri").replace("  BE","").replace("  Kongzili", "").replace("  "," ").replace("Isa Al Masih","Yesus Kristus").replace("Islamijriyah","Islam").replace("Islamijriah","Islam").replace("Hari Raya Natal","Natal").strip()
    if desc.strip() == "Tahun Baru" or desc.strip() == "Tahun Baru  Masehi": return "Tahun Baru Masehi"
    if "Hari Kemerdekaan Republik Indonesia" in desc or "Hari Kemerdekaan RI ke" in desc: return "Hari Kemerdekaan Republik Indonesia"
    if "Hari Buruh" in desc: return "Hari Buruh Internasional"
    if "Cuti bersama Idul Fitri" in desc or "Cuti Bersama Idul Fitri" in desc or "Cuti Bersama Hari Raya Idul Fitri" in desc: return "Cuti Bersama Hari Raya Idul Fitri"
    if desc.strip() == "Hari Raya Idul Fitri" or "Hari Raya Idul Fitriijriyah" in desc or "Hari Raya Idul fitriijriah" in desc: return "Idul Fitri"
    if desc.strip() == "Hari Raya Idul Adha" or "Hari Raya Idul Adhaijriah" in desc or "Hari Raya Idul Adhaijriyah"in desc: return "Idul Adha"
    if "Cuti Bersama Hari Raya Idul Adha" in desc: return "Cuti Bersama Idul Adha"
    if "Hari Paskah" in desc: return "Kebangkitan Yesus Kristus"
    if "pilkada" in desc.lower(): return "Pilkada"
    return desc.strip()
    
def scrape_year(year):
    '''
        SCRAPPING DARI WEBSITE BASE_URL
    '''

    url = f"{BASE_URL}{year}"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    results = []
    paskah_ada = False
    wafat_yesus_date = None
    for month in MONTHS:
        section = soup.find("a", href=f"{month}-{year}")
        if not section: continue
        ul = section.find_parent("ul")
        red_dates = [int(a.text.strip()) for a in ul.find_all("a", style="color: #f00;") if a.text.strip().isdigit()]
        holiday_map = {}
        table = ul.find("table")
        if table:
            for row in table.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) == 2:
                    days = parse_holiday_range(tds[0].text.strip())
                    desc = clean_keterangan(tds[1].text.strip())
                    if "Kebangkitan Yesus Kristus" in desc: paskah_ada = True
                    if "Wafat Yesus Kristus" in desc and days:
                        wafat_yesus_date = datetime(year, MONTHS.index(month) + 1, days[0])
                    for d in days: holiday_map[d] = desc
        for day in red_dates:
            try:
                date_obj = datetime(year, MONTHS.index(month) + 1, day)
                date_str = date_obj.strftime('%Y-%m-%d')
                weekday = date_obj.strftime('%A')
                keterangan = holiday_map.get(day, '')
                results.append((date_str, weekday, keterangan))
            except ValueError: continue
    if not paskah_ada and wafat_yesus_date:
        easter_date = wafat_yesus_date + timedelta(days=2)
        results.append((easter_date.strftime('%Y-%m-%d'), easter_date.strftime('%A'), "Kebangkitan Yesus Kristus"))
    return results

def create_events_excel_file(start_year,end_year):
    '''
        buat event ramadan dan ujian(sebenernya buat kalau mau nambahin rentang liburan)
    '''
    if os.path.exists(EVENTS_EXCEL_FILE): return
    all_holiday_data = [item for year in range(start_year, end_year+1) for item in scrape_year(year)]
    holidays_df = pd.DataFrame(all_holiday_data, columns=['Date', 'Weekday', 'Holiday Name'])
    holidays_df.drop(columns=['Weekday'], inplace=True)
    holidays_df.drop_duplicates(subset=['Date', 'Holiday Name'], keep='first', inplace=True)
    holidays_df['Date'] = pd.to_datetime(holidays_df['Date'])
    ramadan_data_str = """
    2022-04-02,2022-05-01
    2023-03-22,2023-04-21
    2024-03-10,2024-04-09
    2025-03-01,2025-03-30
    2026-02-17,2026-03-19

    """
    ramadan_data = [line.strip().split(',') for line in ramadan_data_str.strip().split('\n') if line.strip()]
    ramadan_df = pd.DataFrame(ramadan_data, columns=['Start Date', 'End Date'])
    ramadan_df['Start Date'] = pd.to_datetime(ramadan_df['Start Date'])
    ramadan_df['End Date'] = pd.to_datetime(ramadan_df['End Date'])
    ujian_data_str = """
    2022-06-13,2022-06-17
    2022-12-05,2022-12-09
    2023-06-12,2023-06-16
    2023-12-04,2023-12-08
    2024-06-10,2024-06-14
    2024-12-09,2024-12-13
    2025-06-16,2025-06-20

    """
    ujian_data = [line.strip().split(',') for line in ujian_data_str.strip().split('\n') if line.strip()]
    ujian_df = pd.DataFrame(ujian_data, columns=['Start Date', 'End Date'])
    ujian_df['Start Date'] = pd.to_datetime(ujian_df['Start Date'])
    ujian_df['End Date'] = pd.to_datetime(ujian_df['End Date'])
    with pd.ExcelWriter(EVENTS_EXCEL_FILE, engine='xlsxwriter') as writer:
        holidays_df.to_excel(writer, sheet_name='Holidays', index=False)
        ramadan_df.to_excel(writer, sheet_name='Ramadan', index=False)
        ujian_df.to_excel(writer, sheet_name='Ujian', index=False)

def update_holidays_data(start_year,end_year):
    '''
        RE SCRAPING DARI WEBSITE
    '''
        
    st.info(f"Mulai scraping hari libur dari tahun **{start_year}** hingga **{end_year}**...")
    new_holiday_data = [item for year in range(start_year, end_year + 1) for item in scrape_year(year)]
    if not new_holiday_data:
        st.warning("Tidak ada data baru yang ditemukan dari scraping.")
        return False
        
    new_holidays_df = pd.DataFrame(new_holiday_data, columns=['Date', 'Weekday', 'Holiday Name'])
    new_holidays_df.drop(columns=['Weekday'], inplace=True)
    st.write(new_holidays_df)
    new_holidays_df.drop_duplicates(subset=['Date', 'Holiday Name'], keep='first', inplace=True)
    new_holidays_df['Date'] = pd.to_datetime(new_holidays_df['Date'])
    
    try:
        existing_df = pd.read_excel(EVENTS_EXCEL_FILE, sheet_name='Holidays')
        existing_df['Date'] = pd.to_datetime(existing_df['Date'])
    except Exception as e:
        st.error(f"Gagal memuat sheet 'Holidays'. Error: {e}")
        return False
        
    merged_df = pd.concat([existing_df, new_holidays_df], ignore_index=True)
    final_df = merged_df.drop_duplicates(subset=['Date', 'Holiday Name'], keep='first')
    final_df.sort_values(by='Date', inplace=True)
    final_df.reset_index(drop=True, inplace=True)

    return save_changes(final_df, sheet_name='Holidays', all_sheets=['Holidays', 'Ramadan', 'Ujian'])
    


# @st.cache_data
def load_data(sheet_name):
    try:
        df = pd.read_excel(EVENTS_EXCEL_FILE, sheet_name=sheet_name)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.date
        if 'Start Date' in df.columns and 'End Date' in df.columns:
            df['Start Date'] = pd.to_datetime(df['Start Date']).dt.date
            df['End Date'] = pd.to_datetime(df['End Date']).dt.date
        return df
    except Exception as e:
        st.error(f"Gagal memuat data dari sheet '{sheet_name}'. Pastikan nama sheet benar. Error: {e}")
        return pd.DataFrame()

def save_changes(df, sheet_name, all_sheets):
    try:
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            for name in all_sheets:
                if name == sheet_name:
                    temp_df = df.copy()
                    for col in temp_df.columns:
                        if 'date' in col.lower() and pd.api.types.is_datetime64_any_dtype(temp_df[col]):
                            temp_df[col] = temp_df[col].dt.strftime('%Y-%m-%d')
                    temp_df.to_excel(writer, sheet_name=name, index=False)
                else:
                    other_df = pd.read_excel(EVENTS_EXCEL_FILE, sheet_name=name)
                    other_df.to_excel(writer, sheet_name=name, index=False)
        with open(EVENTS_EXCEL_FILE, 'wb') as f:
            f.write(excel_buffer.getbuffer())
        st.success("✅ Perubahan berhasil disimpan!")
        return True
    except Exception as e:
        st.error(f"❌ Terjadi kesalahan saat menyimpan file: {e}")
        return False

def add_holiday_form(edited_df,new_date,new_description):
    if not new_date or not new_description:
        st.warning("Mohon isi semua field untuk menambahkan data.")
        return None

    new_row = pd.DataFrame([{'Date': new_date, 'Holiday Name': new_description}])
    updated_df = pd.concat([edited_df, new_row], ignore_index=True)
    updated_df.sort_values(by='Date', inplace=True)
    updated_df.reset_index(drop=True, inplace=True)
    st.session_state['temp_df'] = updated_df
    save_changes(updated_df, sheet_name='Holidays', all_sheets=['Holidays', 'Ramadan', 'Ujian'])

    return updated_df

def validate_dates(start_date, end_date):
    return start_date and end_date and start_date <= end_date

def add_period(edited_df, start_date, end_date,selected_sheet):
    new_row = pd.DataFrame([{'Start Date': start_date, 'End Date': end_date}])
    updated_df = pd.concat([edited_df, new_row], ignore_index=True)
    updated_df.sort_values(by='Start Date', inplace=True)
    updated_df.reset_index(drop=True, inplace=True)
    st.session_state['temp_df'] = updated_df
    save_changes(updated_df, selected_sheet, all_sheets=['Holidays', 'Ramadan', 'Ujian'])

    return updated_df
