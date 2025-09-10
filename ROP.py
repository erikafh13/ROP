import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import math
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import os
from datetime import datetime, timedelta

# --- KONFIGURASI & KONSTANTA ---
st.set_page_config(layout="wide", page_title="Analisis Stock & ROP")

# Konstanta untuk Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_ID_PENJUALAN = "1wH9o4dyNfjve9ScJ_DB2TwT0EDsPe9Zf"
FOLDER_ID_PRODUK = "1UdGbFzZ2Wv83YZLNwdU-rgY-LXlczsFv"

# Konstanta untuk perhitungan
LEAD_TIME_DAYS = 21
ANALYSIS_WINDOW_DAYS = 90 # Jendela waktu untuk menghitung WMA & Std Dev
LEAD_TIME_RATIO = LEAD_TIME_DAYS / 30.0 # Diasumsikan basis WMA adalah per 30 hari
SAFETY_STOCK_FACTOR = math.sqrt(LEAD_TIME_RATIO) # sqrt(21/30) -> sqrt(0.7)

# --- Inisialisasi Session State ---
def initialize_session_state():
    """Menginisialisasi semua variabel session state yang dibutuhkan."""
    if 'df_penjualan' not in st.session_state:
        st.session_state.df_penjualan = pd.DataFrame()
    if 'produk_ref' not in st.session_state:
        st.session_state.produk_ref = pd.DataFrame()
    if 'rop_analysis_result' not in st.session_state:
        st.session_state.rop_analysis_result = None
    if 'error_analysis_result' not in st.session_state:
        st.session_state.error_analysis_result = None
    if 'drive_service' not in st.session_state:
        st.session_state.drive_service = None
        st.session_state.drive_available = False

# --------------------------------Fungsi Utilitas & Google Drive--------------------------------

def connect_to_gdrive():
    """Membangun koneksi ke Google Drive dan menyimpannya di session state."""
    try:
        credentials = None
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], scopes=SCOPES
            )
        elif os.path.exists("credentials.json"):
            credentials = service_account.Credentials.from_service_account_file(
                'credentials.json', scopes=SCOPES
            )
        
        if credentials:
            st.session_state.drive_service = build('drive', 'v3', credentials=credentials)
            st.session_state.drive_available = True
            st.sidebar.success("Terhubung ke Google Drive.", icon="☁️")
        else:
            st.sidebar.error("Kredensial Google Drive tidak ditemukan.")
            st.session_state.drive_available = False
    except Exception as e:
        st.sidebar.error("Gagal terhubung ke Google Drive.")
        st.error(f"Detail Error: {e}")
        st.session_state.drive_available = False

@st.cache_data(ttl=600)
def list_files_in_folder(_drive_service, folder_id):
    """Mencari daftar file dalam folder Google Drive."""
    if not st.session_state.drive_available: return []
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    response = _drive_service.files().list(q=query, fields="files(id, name)").execute()
    return response.get('files', [])

@st.cache_data(ttl=600)
def download_file_from_gdrive(file_id):
    """Mengunduh file dari Google Drive berdasarkan ID."""
    request = st.session_state.drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def read_data_from_gdrive(file_id, file_name, is_produk=False):
    """Membaca file CSV atau Excel dari Google Drive."""
    try:
        fh = download_file_from_gdrive(file_id)
        if is_produk:
            df = pd.read_excel(fh, sheet_name="Sheet1 (2)", skiprows=6, usecols=[0, 1, 2, 3])
            df.columns = ['No. Barang', 'BRAND Barang', 'Kategori Barang', 'Nama Barang']
        elif file_name.endswith('.csv'):
            df = pd.read_csv(fh)
        else:
            df = pd.read_excel(fh)
        
        if 'No. Barang' in df.columns:
            df['No. Barang'] = df['No. Barang'].astype(str).str.strip()
        return df
    except Exception as e:
        st.error(f"Gagal membaca file {file_name}. Error: {e}")
        return pd.DataFrame()

@st.cache_data
def convert_df_to_excel(df):
    """Mengonversi DataFrame ke format Excel (bytes)."""
    output = BytesIO()
    df_to_save = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df_to_save.columns = ['_'.join(map(str, col)).strip() for col in df_to_save.columns.values]
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_save.to_excel(writer, index=True, sheet_name='Sheet1')
    return output.getvalue()

# --------------------------------Fungsi Preprocessing & Analisis--------------------------------

def map_nama_dept(row):
    dept = str(row.get('Dept.', '')).strip().upper()
    pelanggan = str(row.get('Nama Pelanggan', '')).strip().upper()
    if dept == 'A':
        return 'A - ITC' if pelanggan in ['A - CASH', 'AIRPAY INTERNATIONAL INDONESIA', 'TOKOPEDIA'] else 'A - RETAIL'
    mapping = {'B': 'B - JKT', 'C': 'C - PUSAT', 'D': 'D - SMG','E': 'E - JOG', 'F': 'F - MLG', 'G': 'G - PROJECT','H': 'H - BALI'}
    return mapping.get(dept, 'X')

def map_city(nama_dept):
    mapping = {
        'A - ITC': 'Surabaya', 'A - RETAIL': 'Surabaya', 'C - PUSAT': 'Surabaya', 'G - PROJECT': 'Surabaya',
        'B - JKT': 'Jakarta', 'D - SMG': 'Semarang', 'E - JOG': 'Jogja', 'F - MLG': 'Malang', 'H - BALI': 'Bali'
    }
    return mapping.get(nama_dept, 'Others')

@st.cache_data
def preprocess_penjualan_data(df):
    """Melakukan semua langkah preprocessing pada data penjualan."""
    if df.empty: return pd.DataFrame()
    df_processed = df.copy()
    
    if 'Qty' in df_processed.columns and 'Kuantitas' not in df_processed.columns:
        df_processed.rename(columns={'Qty': 'Kuantitas'}, inplace=True)
    
    if 'Kuantitas' not in df_processed.columns:
        st.error("Kolom 'Kuantitas' atau 'Qty' tidak ditemukan di data penjualan.")
        return pd.DataFrame()

    df_processed['Nama Dept'] = df_processed.apply(map_nama_dept, axis=1)
    df_processed['City'] = df_processed['Nama Dept'].apply(map_city)
    df_processed = df_processed[df_processed['City'] != 'Others'].copy()
    df_processed['Tgl Faktur'] = pd.to_datetime(df_processed['Tgl Faktur'], errors='coerce')
    df_processed.dropna(subset=['Tgl Faktur'], inplace=True)
    return df_processed

@st.cache_data(ttl=3600)
def calculate_base_metrics(_penjualan_df, _produk_df, start_date, end_date):
    """Menghitung metrik dasar (WMA, std dev, dll.) yang digunakan untuk semua metode ROP."""
    analysis_start_date = pd.to_datetime(start_date) - pd.DateOffset(days=ANALYSIS_WINDOW_DAYS)
    extended_end_date = pd.to_datetime(end_date) + pd.DateOffset(days=LEAD_TIME_DAYS)
    date_range_full = pd.date_range(start=analysis_start_date, end=extended_end_date, freq='D')
    
    daily_sales = _penjualan_df.groupby(['Tgl Faktur', 'City', 'No. Barang'])['Kuantitas'].sum().reset_index()
    daily_sales.rename(columns={'Tgl Faktur': 'Date'}, inplace=True)
    daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])

    def process_group(group):
        group = group.set_index('Date').reindex(date_range_full, fill_value=0)
        group.rename(columns={'Kuantitas': 'SO'}, inplace=True)
        
        sales_30d = group['SO'].rolling(window=30, min_periods=1).sum()
        sales_60d = group['SO'].rolling(window=60, min_periods=1).sum()
        sales_90d = group['SO'].rolling(window=90, min_periods=1).sum()
        
        group['WMA'] = (sales_30d * 0.5) + ((sales_60d - sales_30d) * 0.3) + ((sales_90d - sales_60d) * 0.2)
        group['std_dev_90d'] = group['SO'].rolling(window=ANALYSIS_WINDOW_DAYS, min_periods=1).std().fillna(0)
        group['Penjualan_Riil_21_Hari'] = group['SO'].iloc[::-1].rolling(window=LEAD_TIME_DAYS, min_periods=0).sum().iloc[::-1].shift(-LEAD_TIME_DAYS)
        return group

    processed_data = daily_sales.groupby(['City', 'No. Barang'], group_keys=False).apply(process_group).reset_index()
    processed_data.rename(columns={'index': 'Date'}, inplace=True)
    processed_data['Date'] = pd.to_datetime(processed_data['Date'])
    
    # Klasifikasi ABC
    avg_sales = processed_data.groupby(['City', 'No. Barang'])['WMA'].mean().reset_index()
    def classify_abc(df_city):
        df_city = df_city.sort_values(by='WMA', ascending=False)
        total_sales = df_city['WMA'].sum()
        if total_sales > 0:
            df_city['Cumulative_Perc'] = 100 * df_city['WMA'].cumsum() / total_sales
            df_city['Kategori ABC'] = pd.cut(df_city['Cumulative_Perc'], bins=[-1, 70, 90, 101], labels=['A', 'B', 'C'], right=True)
        else:
            df_city['Kategori ABC'] = 'D'
        return df_city[['City', 'No. Barang', 'Kategori ABC']]

    abc_classification = avg_sales.groupby('City', group_keys=False).apply(classify_abc).reset_index(drop=True)
    final_df = pd.merge(processed_data, abc_classification, on=['City', 'No. Barang'], how='left')
    
    final_df = pd.merge(final_df, _produk_df, on='No. Barang', how='left')
    
    # Filter tanggal sesuai input pengguna
    final_df = final_df[
        (final_df['Date'].dt.date >= pd.to_datetime(start_date).date()) & 
        (final_df['Date'].dt.date <= pd.to_datetime(end_date).date())
    ].copy()
    
    final_df['SO'] = final_df['SO'].astype(int)
    
    return final_df

def apply_rop_calculation(df, method):
    """Menerapkan perhitungan ROP berdasarkan metode yang dipilih."""
    df_rop = df.copy()
    
    if method == "ABC Bertingkat":
        z_scores = {'A': 1.65, 'B': 1.0, 'C': 0.0, 'D': 0.0}
    elif method == "Uniform":
        z_scores = {'A': 1.0, 'B': 1.0, 'C': 1.0, 'D': 1.0}
    else: # ROP = Min Stock
        z_scores = {'A': 0.0, 'B': 0.0, 'C': 0.0, 'D': 0.0}
    
    df_rop['Z_Score'] = df_rop['Kategori ABC'].map(z_scores).fillna(0).astype(float)
    df_rop['Safety Stock'] = df_rop['Z_Score'] * df_rop['std_dev_90d'] * SAFETY_STOCK_FACTOR
    df_rop['Min Stock'] = df_rop['WMA'] * LEAD_TIME_RATIO
    df_rop['ROP'] = (df_rop['Min Stock'] + df_rop['Safety Stock']).round().astype(int)
    
    return_cols = ['Date', 'City', 'No. Barang', 'Kategori Barang', 'BRAND Barang', 'Nama Barang', 'ROP', 'SO', 'Penjualan_Riil_21_Hari']
    return df_rop[return_cols]

# --------------------------------Fungsi untuk Render Halaman--------------------------------

def render_input_page():
    """Merender halaman untuk input data."""
    st.title("📥 Input Data")
    st.markdown("Muat atau muat ulang data yang diperlukan dari Google Drive.")

    if not st.session_state.drive_available:
        st.warning("Tidak dapat melanjutkan karena koneksi ke Google Drive gagal.")
        st.stop()
    
    # --- Data Penjualan ---
    st.header("1. Data Penjualan")
    with st.spinner("Mencari file penjualan..."):
        penjualan_files_list = list_files_in_folder(st.session_state.drive_service, FOLDER_ID_PENJUALAN)
    
    if st.button("Muat / Muat Ulang Data Penjualan"):
        if penjualan_files_list:
            all_dfs = []
            progress_bar = st.progress(0, "Memulai pengunduhan...")
            for i, f in enumerate(penjualan_files_list):
                progress_bar.progress((i + 1) / len(penjualan_files_list), f"Mengunduh & membaca {f['name']}...")
                all_dfs.append(read_data_from_gdrive(f['id'], f['name']))
            
            st.session_state.df_penjualan = pd.concat(all_dfs, ignore_index=True)
            st.success("Data penjualan berhasil dimuat ulang.")
        else:
            st.warning("⚠️ Tidak ada file penjualan ditemukan.")

    if not st.session_state.df_penjualan.empty:
        df = st.session_state.df_penjualan
        st.success(f"✅ Data penjualan telah dimuat ({len(df)} baris).")
        df['Tgl Faktur'] = pd.to_datetime(df['Tgl Faktur'], errors='coerce')
        min_date, max_date = df['Tgl Faktur'].min(), df['Tgl Faktur'].max()
        if pd.notna(min_date) and pd.notna(max_date):
            st.info(f"📅 **Rentang Data:** Dari **{min_date.strftime('%d %B %Y')}** hingga **{max_date.strftime('%d %B %Y')}**.")
        st.dataframe(df.head())

    # --- Produk Referensi ---
    st.header("2. Produk Referensi")
    with st.spinner("Mencari file produk..."):
        produk_files_list = list_files_in_folder(st.session_state.drive_service, FOLDER_ID_PRODUK)
        
    selected_file = st.selectbox("Pilih file Produk:", options=[None] + produk_files_list, format_func=lambda x: x['name'] if x else "Pilih file")
    
    if selected_file:
        with st.spinner(f"Memuat {selected_file['name']}..."):
            st.session_state.produk_ref = read_data_from_gdrive(selected_file['id'], selected_file['name'], is_produk=True)
            st.success(f"File produk '{selected_file['name']}' berhasil dimuat.")
    
    if not st.session_state.produk_ref.empty:
        st.dataframe(st.session_state.produk_ref.head())

def render_rop_analysis_page():
    """Merender halaman untuk hasil analisis ROP."""
    st.title("📈 Hasil Analisa ROP & Sell Out")
    
    metode_rop = st.sidebar.selectbox("Pilih Metode Perhitungan ROP:", ("ABC Bertingkat", "Uniform", "ROP = Min Stock"))

    if st.session_state.df_penjualan.empty or st.session_state.produk_ref.empty:
        st.warning("⚠️ Harap muat file **Penjualan** dan **Produk Referensi** di halaman **'Input Data'**.")
        st.stop()
    
    penjualan_clean = preprocess_penjualan_data(st.session_state.df_penjualan)
    
    st.header("Pilih Rentang Tanggal untuk Analisis")
    default_end = penjualan_clean['Tgl Faktur'].max().date()
    default_start = default_end - timedelta(days=6)
    
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Tanggal Awal", value=default_start, key="rop_start")
    end_date = col2.date_input("Tanggal Akhir", value=default_end, key="rop_end")

    if st.button("🚀 Jalankan Analisa ROP & SO 🚀"):
        if start_date > end_date:
            st.error("Tanggal Awal tidak boleh melebihi Tanggal Akhir.")
        else:
            with st.spinner("Menghitung ROP & SO..."):
                base_metrics_df = calculate_base_metrics(penjualan_clean, st.session_state.produk_ref, start_date, end_date)
                rop_result_df = apply_rop_calculation(base_metrics_df, metode_rop)
                st.session_state.rop_analysis_result = rop_result_df
                st.success("Analisis berhasil dijalankan!")

    if st.session_state.rop_analysis_result is not None:
        result_df = st.session_state.rop_analysis_result
        # ... (Kode filtering dan display tabel pivot, disarankan pakai st.dataframe) ...
        st.header("Tabel ROP & SO per Kota")
        unique_cities = sorted(result_df['City'].dropna().unique())
        
        pivot_outputs = {}
        for city in unique_cities:
            with st.expander(f"📍 Hasil untuk Kota: {city}", expanded=(city == "Surabaya")):
                city_df = result_df[result_df['City'] == city].copy()
                if not city_df.empty:
                    pivot_city = city_df.pivot_table(
                        index=['No. Barang', 'Nama Barang', 'BRAND Barang', 'Kategori Barang'],
                        columns=city_df['Date'].dt.strftime('%Y-%m-%d'),
                        values=['ROP', 'SO']
                    ).fillna(0).astype(int)
                    
                    pivot_city.columns = pivot_city.columns.swaplevel(0, 1)
                    pivot_city.sort_index(axis=1, level=0, inplace=True)
                    
                    pivot_outputs[f"ROP_{city.replace(' ', '_')}"] = pivot_city
                    st.dataframe(pivot_city) # Lebih baik dari to_html()
                else:
                    st.write("Tidak ada data untuk kota ini dengan filter yang dipilih.")
        
        # --- Tombol Download ---
        if pivot_outputs:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for sheet_name, df_pivot in pivot_outputs.items():
                    df_pivot.to_excel(writer, sheet_name=sheet_name)
            st.download_button("📥 Unduh Semua Hasil (Excel)", output.getvalue(), f"hasil_rop_so_{start_date}_to_{end_date}.xlsx")


def render_error_analysis_page():
    """Merender halaman untuk analisis error metode ROP."""
    st.title("🎯 Analisis Error Metode ROP")

    if st.session_state.df_penjualan.empty or st.session_state.produk_ref.empty:
        st.warning("⚠️ Harap muat file **Penjualan** dan **Produk Referensi** di halaman **'Input Data'**.")
        st.stop()
        
    penjualan_clean = preprocess_penjualan_data(st.session_state.df_penjualan)

    st.header("Pilih Rentang Tanggal untuk Analisis Error")
    st.info("Pastikan data penjualan Anda mencakup 21 hari setelah tanggal akhir yang dipilih.")
    
    default_end = penjualan_clean['Tgl Faktur'].max().date() - timedelta(days=LEAD_TIME_DAYS)
    default_start = default_end - timedelta(days=29)
    
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Tanggal Awal", value=default_start, key="err_start")
    end_date = col2.date_input("Tanggal Akhir", value=default_end, key="err_end")
    
    if st.button("🚀 Jalankan Analisis Error 🚀"):
        with st.spinner("Menjalankan analisis... Ini mungkin butuh waktu."):
            progress_bar = st.progress(0, text="Menghitung metrik dasar...")
            base_metrics = calculate_base_metrics(penjualan_clean, st.session_state.produk_ref, start_date, end_date)
            
            progress_bar.progress(33, text="Menghitung ROP ABC Bertingkat...")
            rop_abc = apply_rop_calculation(base_metrics, "ABC Bertingkat").rename(columns={'ROP': 'ROP_ABC'})
            
            progress_bar.progress(66, text="Menghitung ROP Uniform...")
            rop_uniform = apply_rop_calculation(base_metrics, "Uniform").rename(columns={'ROP': 'ROP_Uniform'})
            
            progress_bar.progress(90, text="Menghitung ROP Min Stock...")
            rop_min = apply_rop_calculation(base_metrics, "ROP = Min Stock").rename(columns={'ROP': 'ROP_Min_Stock'})

            # Gabungkan hasil
            final_df = rop_abc.drop(columns=['ROP_ABC']).copy()
            final_df['ROP_ABC'] = rop_abc['ROP']
            final_df['ROP_Uniform'] = rop_uniform['ROP']
            final_df['ROP_Min_Stock'] = rop_min['ROP']

            final_df.dropna(subset=['Penjualan_Riil_21_Hari'], inplace=True)
            final_df['Error_ABC'] = (final_df['Penjualan_Riil_21_Hari'] - final_df['ROP_ABC']).abs()
            final_df['Error_Uniform'] = (final_df['Penjualan_Riil_21_Hari'] - final_df['ROP_Uniform']).abs()
            final_df['Error_Min_Stock'] = (final_df['Penjualan_Riil_21_Hari'] - final_df['ROP_Min_Stock']).abs()
            
            st.session_state.error_analysis_result = final_df
            progress_bar.progress(100, text="Analisis Selesai!")

    if st.session_state.error_analysis_result is not None:
        result_df = st.session_state.error_analysis_result
        st.header("🏆 Hasil Perbandingan Metode")
        
        summary_data = {
            'Metode ROP': ['ABC Bertingkat', 'Uniform', 'ROP = Min Stock'],
            'Rata-Rata Error (MAE)': [
                result_df['Error_ABC'].mean(),
                result_df['Error_Uniform'].mean(),
                result_df['Error_Min_Stock'].mean()
            ]
        }
        summary_df = pd.DataFrame(summary_data).set_index('Metode ROP').round(2)
        
        st.subheader("Skor Error Keseluruhan (Semakin Kecil Semakin Baik)")
        st.dataframe(summary_df.style.highlight_min(color='lightgreen', axis=0))
        st.bar_chart(summary_df)


# --------------------------------Fungsi Utama Aplikasi--------------------------------
def main():
    """Fungsi utama untuk menjalankan aplikasi Streamlit."""
    initialize_session_state()
    
    st.sidebar.image("https://i.imgur.com/n0KzG1p.png", use_container_width=True)
    st.sidebar.title("Analisis Stock dan ROP")
    
    # Hanya hubungkan ke GDrive sekali saat aplikasi dimulai
    if st.session_state.drive_service is None:
        connect_to_gdrive()
    
    page = st.sidebar.radio(
        "Menu Navigasi:",
        ("Input Data", "Hasil Analisa ROP", "Analisis Error Metode ROP")
    )
    st.sidebar.markdown("---")

    if page == "Input Data":
        render_input_page()
    elif page == "Hasil Analisa ROP":
        st.sidebar.header("🔧 Pengaturan Metode ROP")
        render_rop_analysis_page()
    elif page == "Analisis Error Metode ROP":
        render_error_analysis_page()

if __name__ == "__main__":
    main()
