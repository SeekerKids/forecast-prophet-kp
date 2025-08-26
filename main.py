import streamlit as st
import modules.event_manager as event_manager
import modules.prophet_app as prophet_app
from modules.utils import inject_css

def main_app():
    st.set_page_config(layout="wide")
    inject_css("assets/style.css")
    st.sidebar.title("Navigasi Aplikasi")
    app_mode = st.sidebar.selectbox("Pilih Mode:", ["Pengelola Events", "Prediksi Penjualan"])
    
    if app_mode == "Pengelola Events":
        event_manager.run()
    elif app_mode == "Prediksi Penjualan":
        prophet_app.run()

if __name__ == "__main__":
    main_app()