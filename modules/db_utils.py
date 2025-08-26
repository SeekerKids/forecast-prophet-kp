import streamlit as st
import pyodbc
import pandas as pd

# Konfigurasi Database SQL Server
# DB_SERVER = "LAPTOP-KMG2KLUV\SQLEXPRESS"
# DB_USER = "sa"
# DB_PASSWORD = "your_sql_server_password"
# DATABASE_OPTIONS = ["R88B2", "MTH", "KUNINGAN", "R88A", "R88A2"]

def get_server_connection():
    '''
        AMBIL KONEKSI MASTER UNTUK FUNCTION get_database_list
    '''
    try:
        secrets = st.secrets["database"]
        conn_str = (
            f"DRIVER={{{secrets['driver']}}};"
            f"SERVER={secrets['server']};"
            f"DATABASE=master;"  # konek ke master untuk akses sys.databases
            f"UID={secrets['user']};"
            f"Trusted_Connection=yes;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        st.error(f"❌ Gagal koneksi ke SQL Server: {e}")
        return None

def get_database_list(conn):
    '''
        MENGAMBIL DAFTAR DATABASE
    '''
    try:
        query = """
        SELECT name 
        FROM sys.databases 
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') 
        ORDER BY name
        """
        df = pd.read_sql(query, conn)
        return df["name"].tolist()
    except Exception as e:
        st.error(f"❌ Gagal mengambil daftar database: {e}")
        return []

def get_db_connection(db_name):
    '''
        KONEKSI KE DATABASE
    '''
    try:
        secrets = st.secrets["database"]
        conn_str = (
            f"DRIVER={{{secrets['driver']}}};"
            f"SERVER={secrets['server']};"
            f"DATABASE={db_name};"
            f"UID={secrets['user']};"
            f"Trusted_Connection=yes;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        st.error(f"❌ Gagal koneksi ke database {db_name}: {e}")
        return None

@st.cache_data
def get_unique_categories(db_name, start_date, end_date):
    '''
        MENGAMBIL KATEGORI UNIK YANG MEMILIKI PENJUALAN SELAMA RENTANG WAKTU SECARA PENUH 
    '''

    query = f"""
    SELECT
        k.namakategori
    FROM
        s_posdetail spd
    JOIN
        i_item a ON spd.ItemID = a.ItemId
    JOIN
        m_kategori k ON a.idkategori = k.idkategori
    WHERE
        CAST(spd.posdate AS DATE) >= '{start_date}'
        AND CAST(spd.posdate AS DATE) <= '{end_date}'
    GROUP BY
        k.namakategori
    HAVING
        COUNT(DISTINCT CAST(spd.posdate AS DATE)) = DATEDIFF(day, '{start_date}', '{end_date}') + 1
    ORDER BY
        SUM(spd.qty) DESC;
    """
    df = run_query(db_name, query)
    if not df.empty:
        return df['namakategori'].dropna().tolist()
    return []

@st.cache_data
def run_query(db_name, query):
    '''
        MENJALANKAN QUERRY SQL
    '''

    conn = None
    try:
        conn = get_db_connection(db_name)
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error saat menjalankan query: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()
        
@st.cache_data
def load_data(db_name, kategori, start_date, end_date):
    '''
        MENGAMBIL DATA DARI DB DENGAN QUERRY YANG SUDAH DITENTUKKAN (1x TRANSAKSI MAX 50 ITEM/ JENIS, MENGHILANGKAN SEMUA PROMO)
    '''

    conn = get_db_connection(db_name)
    if not conn: return pd.DataFrame()
    query = f"""
WITH
DailyCategorySales AS (
    SELECT
        CAST(spd.posdate AS DATE) AS aggregated_date,
        k.namakategori,
        SUM(spd.qty) AS Total_Qty
    FROM s_posdetail spd
    JOIN dbo.i_item a ON spd.ItemID = a.ItemId
    JOIN dbo.m_kategori k ON a.idkategori = k.idkategori
    JOIN dbo.m_subdepartement sk ON k.idsubdepartement = sk.IdSubdepartement
    JOIN dbo.m_department d ON sk.IDdepartement = d.departmentID
    JOIN dbo.m_divisi dv ON dv.iddivisi = d.iddivisi
    JOIN dbo.i_itemcategory ic ON a.categoryid = ic.categoryID
    WHERE
        CAST(spd.posdate AS DATE) >= '{start_date}'
        AND CAST(spd.posdate AS DATE) <= '{end_date}'
        AND spd.Qty <= 50
    GROUP BY
        CAST(spd.posdate AS DATE), k.namakategori
),
RankedSales AS (
    SELECT
        aggregated_date,
        namakategori,
        Total_Qty,
        ROW_NUMBER() OVER (PARTITION BY aggregated_date ORDER BY Total_Qty DESC) AS rank_num
    FROM DailyCategorySales
)
SELECT
    rs.aggregated_date,
    (
        SELECT SUM(spd2.qty)
        FROM s_posdetail spd2
        JOIN dbo.i_item a2 ON spd2.ItemID = a2.ItemId
        JOIN dbo.m_kategori k2 ON a2.idkategori = k2.idkategori
        JOIN dbo.m_subdepartement sk2 ON k2.idsubdepartement = sk2.IdSubdepartement
        JOIN dbo.m_department d2 ON sk2.IDdepartement = d2.departmentID
        JOIN dbo.m_divisi dv2 ON dv2.iddivisi = d2.iddivisi
        WHERE
            CAST(spd2.posdate AS DATE) = rs.aggregated_date
            AND k2.namakategori = '{kategori}'
            AND spd2.Qty <= 50
    ) AS Jumlah
FROM RankedSales rs
WHERE rs.rank_num = 1
ORDER BY aggregated_date;
    """
    try:
        cur = conn.cursor()
        cur.execute(query)
        db_data = cur.fetchall()
        processed_db_data = [tuple(row) for row in db_data]
        columns = ['Date', 'Sales']
        df = pd.DataFrame(processed_db_data, columns=columns)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df['Sales'] = df['Sales'].astype(float)
        return df
    except Exception as e:
        st.error(f"Error saat menjalankan kueri SQL: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()