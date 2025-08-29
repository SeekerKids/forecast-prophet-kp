import streamlit as st
import pyodbc #ssms
import pandas as pd
import psycopg2 #postgree
'''
    SSMS
'''
# Mendefinisikan ulang fungsi koneksi
def get_db_connection_ssms(db_name):
    '''
        KONEKSI KE DATABASE
    '''
    try:
        secrets = st.secrets["ssms"]
        conn_str = (
            f"DRIVER={{{secrets['driver']}}};"
            f"SERVER={secrets['server']};"
            f"DATABASE={secrets['dbname']};"
            f"UID={secrets['user']};"
            f"Trusted_Connection=yes;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        st.error(f"❌ Gagal koneksi ke database {db_name}: {e}")
        return None

@st.cache_data
def get_branch_list_ssms(db_name):
    '''
        MENGAMBIL BRANCH UNIK DARI M_BRANCH
    '''

    query = f"""
        SELECT
            BranchId, BranchName
        FROM m_branch
        WHERE
            active = 1
            and branchtype = 'STORE'
            and branchcategory not in('CLOSED','EVENT')
    """
    df = run_query(db_name, query)
    if not df.empty:
        return df[['BranchId', 'BranchName']].dropna().values.tolist()
    return []

# @st.cache_data
def get_unique_categories_ssms(db_name, start_date, end_date):
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

# @st.cache_data
def run_query(db_name, query):
    '''
        MENJALANKAN QUERRY SQL
    '''

    conn = None
    try:
        conn = get_db_connection_ssms(db_name)
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error saat menjalankan query: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

@st.cache_data
def load_data_ssms(db_name,branchid, kategori, start_date, end_date):
    '''
        MENGAMBIL DATA DARI DB DENGAN QUERRY YANG SUDAH DITENTUKKAN (1x TRANSAKSI MAX 50 ITEM/ JENIS, MENGHILANGKAN SEMUA PROMO)
    '''

    conn = get_db_connection_ssms(db_name)
    if not conn: return pd.DataFrame()
    query = f"""

        SELECT
            CAST(spd.posdate AS DATE) AS SalesDate,
            SUM(spd.qty) AS Jumlah
        FROM
            s_posdetail spd
        JOIN
            S_POS sp ON spd.POSNo = sp.POSNo
        JOIN
            i_item ii ON spd.itemid = ii.itemid
        JOIN
            m_kategori mk ON ii.idkategori = mk.idkategori
        WHERE
            spd.posdate >= '{start_date}'
            AND spd.posdate <= '{end_date}'
            AND sp.BranchId = '{branchid}'
            AND mk.namakategori = '{kategori}'
            AND spd.Qty <= 50
        GROUP BY
            CAST(spd.posdate AS DATE)
        ORDER BY
            SalesDate ASC;
        ;
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

'''
    POSTGREE
'''

def get_db_connection_postgres():
    '''
    KONEKSI KE DATABASE POSTGRESQL MENGGUNAKAN NAMA DB DARI SECRETS.
    '''
    try:
        secrets = st.secrets["postgres"]
        conn_str = (
            f"host={secrets['host']} "
            f"port={secrets['port']} "
            f"dbname={secrets['dbname']} "
            f"user={secrets['user']} "
            f"password={secrets['password']}"
        )
        return psycopg2.connect(conn_str)
    except Exception as e:
        st.error(f"❌ Gagal koneksi ke PostgreSQL: {e}")
        return None

@st.cache_data
def get_branch_list_postgres():
    '''
        MENGAMBIL BRANCH UNIK DARI S_POS2
    '''
    query = f"""
        SELECT DISTINCT Branchid
        FROM s_pos2
        ORDER BY Branchid;

    """
    df = run_query_postgres(query)
    if not df.empty:
        return df[['branchid']].dropna().values.tolist()
    return []    

# @st.cache_data
def get_unique_categories_postgres(start_date, end_date):
    '''
    MENGAMBIL KATEGORI UNIK YANG MEMILIKI PENJUALAN SELAMA RENTANG WAKTU SECARA PENUH.
    '''
    query = f"""
    SELECT
        ic.categoryname
    FROM
        s_pos2detail spd
    JOIN
        i_item a ON spd.itemid = a.itemid
    JOIN
        i_itemcategory ic ON a.categoryid = ic.categoryid
    WHERE
        spd.posdate::date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        ic.categoryname
    HAVING
        COUNT(DISTINCT spd.posdate::date) = ('{end_date}'::date - '{start_date}'::date) + 1
    ORDER BY
        SUM(spd.qty) DESC;
    """
    df = run_query_postgres(query)
    if not df.empty:
        return df['categoryname'].dropna().tolist()
    return []

# @st.cache_data
def run_query_postgres(query):
    '''
    MENJALANKAN QUERRY SQL.
    '''
    conn = None
    try:
        conn = get_db_connection_postgres()
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error saat menjalankan query: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

@st.cache_data
def load_data_postgres(branchid, kategori, start_date, end_date):
    '''
    MENGAMBIL DATA DARI DB DENGAN QUERRY YANG SUDAH DITENTUKKAN.
    '''
    conn = get_db_connection_postgres()
    if not conn: return pd.DataFrame()

    query = f"""
    SELECT
        spd.posdate::date AS SalesDate,
        SUM(spd.qty) AS Jumlah
    FROM
        s_pos2detail spd
    JOIN
        S_POS2 sp ON spd.POSNo = sp.POSNo
    JOIN
        i_item ii ON spd.itemid = ii.itemid
    JOIN
        i_itemcategory ic ON ii.categoryid = ic.categoryid
    WHERE
        spd.posdate::date BETWEEN '{start_date}' AND '{end_date}'
        AND sp.branchid = '{branchid}'
        AND ic.categoryname = '{kategori}'
        AND spd.qty <= 50
    GROUP BY
        spd.posdate::date
    ORDER BY
        SalesDate ASC
    ;
    """

    try:
        cur = conn.cursor()
        cur.execute(query)
        db_data = cur.fetchall()
        columns = ['Date', 'Sales']
        df = pd.DataFrame(db_data, columns=columns)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df['Sales'] = df['Sales'].astype(float)
        return df
    except Exception as e:
        st.error(f"Error saat menjalankan kueri SQL: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()