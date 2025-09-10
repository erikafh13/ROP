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

# Konfigurasi awal halaman Streamlit
st.set_page_config(layout="wide", page_title="Analisis Stock & ROP")

# --- SIDEBAR ---
st.sidebar.image("https://i.imgur.com/n0KzG1p.png", use_container_width=True)
st.sidebar.title("Analisis Stock dan ROP")

page = st.sidebar.radio(
    "Menu Navigasi:",
    ("Input Data", "Hasil Analisa ROP", "Analisis Error Metode ROP"),
    help="Pilih halaman untuk ditampilkan."
)
st.sidebar.markdown("---")

# --- Inisialisasi Session State ---
if 'df_penjualan' not in st.session_state:
    st.session_state.df_penjualan = pd.DataFrame()
if 'produk_ref' not in st.session_state:
    st.session_state.produk_ref = pd.DataFrame()
if 'rop_analysis_result' not in st.session_state:
    st.session_state.rop_analysis_result = None

# --------------------------------Fungsi Umum & Google Drive--------------------------------

# --- KONEKSI GOOGLE DRIVE ---
SCOPES = ['https://www.googleapis.com/auth/drive']
DRIVE_AVAILABLE = False
try:
    if "gcp_service_account" in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=SCOPES
        )
        st.sidebar.success("Terhubung ke Google Drive.", icon="☁️")
    elif os.path.exists("credentials.json"):
        credentials = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=SCOPES
        )
        st.sidebar.success("Terhubung ke Google Drive.", icon="💻")
    else:
        st.sidebar.error("Kredensial Google Drive tidak ditemukan.")
        credentials = None

    if credentials:
        drive_service = build('drive', 'v3', credentials=credentials)
        folder_penjualan = "1wH9o4dyNfjve9ScJ_DB2TwT0EDsPe9Zf" 
        folder_produk = "1UdGbFzZ2Wv83YZLNwdU-rgY-LXlczsFv"
        DRIVE_AVAILABLE = True

except Exception as e:
    st.sidebar.error(f"Gagal terhubung ke Google Drive.")
    st.error(f"Detail Error: {e}")


@st.cache_data(ttl=600)
def list_files_in_folder(_drive_service, folder_id):
    if not DRIVE_AVAILABLE: return []
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    response = _drive_service.files().list(q=query, fields="files(id, name)").execute()
    return response.get('files', [])

@st.cache_data(ttl=600)
def download_file_from_gdrive(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def download_and_read(file_id, file_name, **kwargs):
    fh = download_file_from_gdrive(file_id)
    return pd.read_csv(fh, **kwargs) if file_name.endswith('.csv') else pd.read_excel(fh, **kwargs)

def read_produk_file(file_id):
    fh = download_file_from_gdrive(file_id)
    df = pd.read_excel(fh, sheet_name="Sheet1 (2)", skiprows=6, usecols=[0, 1, 2, 3])
    df.columns = ['No. Barang', 'BRAND Barang', 'Kategori Barang', 'Nama Barang']
    return df

# --- FUNGSI MAPPING DATA ---
def map_nama_dept(row):
    dept = str(row.get('Dept.', '')).strip().upper()
    pelanggan = str(row.get('Nama Pelanggan', '')).strip().upper()
    if dept == 'A':
        if pelanggan in ['A - CASH', 'AIRPAY INTERNATIONAL INDONESIA', 'TOKOPEDIA']: return 'A - ITC'
        else: return 'A - RETAIL'
    mapping = {'B': 'B - JKT', 'C': 'C - PUSAT', 'D': 'D - SMG','E': 'E - JOG', 'F': 'F - MLG', 'G': 'G - PROJECT','H': 'H - BALI', 'X': 'X'}
    return mapping.get(dept, 'X')

def map_city(nama_dept):
    if nama_dept in ['A - ITC', 'A - RETAIL', 'C - PUSAT', 'G - PROJECT']: 
        return 'Surabaya'
    elif nama_dept == 'B - JKT': return 'Jakarta'
    elif nama_dept == 'D - SMG': return 'Semarang'
    elif nama_dept == 'E - JOG': return 'Jogja'
    elif nama_dept == 'F - MLG': return 'Malang'
    elif nama_dept == 'H - BALI': return 'Bali'
    else: return 'Others'

# --- FUNGSI KONVERSI EXCEL ---
@st.cache_data
def convert_df_to_excel(df):
    output = BytesIO()
    if isinstance(df.columns, pd.MultiIndex):
        df_to_save = df.copy()
        df_to_save.columns = ['_'.join(map(str, col)).strip() for col in df_to_save.columns.values]
    else:
        df_to_save = df
        
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_save.to_excel(writer, index=True, sheet_name='Sheet1')
    processed_data = output.getvalue()
    return processed_data
    
# =====================================================================================
#                                       HALAMAN INPUT DATA
# =====================================================================================

if page == "Input Data":
    st.title("📥 Input Data")
    st.markdown("Muat atau muat ulang data yang diperlukan dari Google Drive.")
    # ... (Kode di halaman ini tidak berubah) ...

# =====================================================================================
#                                    HALAMAN HASIL ANALISA ROP
# =====================================================================================
elif page == "Hasil Analisa ROP":
    st.title("📈 Hasil Analisa ROP & Sell Out")

    # --- [BARU] Sidebar untuk memilih metode ROP ---
    st.sidebar.header("🔧 Pengaturan Metode ROP")
    metode_rop = st.sidebar.selectbox(
        "Pilih Metode Perhitungan ROP:",
        ("ABC Bertingkat", "Uniform", "ROP = Min Stock")
    )

    @st.cache_data(ttl=3600)
    def calculate_rop_and_sellout(penjualan_df, produk_df, start_date, end_date, method):
        analysis_start_date = pd.to_datetime(start_date) - pd.DateOffset(days=90)
        date_range_full = pd.date_range(start=analysis_start_date, end=end_date, freq='D')
        
        daily_sales = penjualan_df.groupby(['Tgl Faktur', 'City', 'No. Barang'])['Kuantitas'].sum().reset_index()
        daily_sales.rename(columns={'Tgl Faktur': 'Date'}, inplace=True)
        daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])

        def process_group(group):
            group = group.set_index('Date').reindex(date_range_full, fill_value=0)
            group.rename(columns={'Kuantitas': 'SO'}, inplace=True)
            sales_30d = group['SO'].rolling(window=30, min_periods=1).sum()
            sales_60d = group['SO'].rolling(window=60, min_periods=1).sum()
            sales_90d = group['SO'].rolling(window=90, min_periods=1).sum()
            std_dev_90d = group['SO'].rolling(window=90, min_periods=1).std().fillna(0)
            
            group['WMA'] = (sales_30d * 0.5) + ((sales_60d - sales_30d) * 0.3) + ((sales_90d - sales_60d) * 0.2)
            group['std_dev_90d'] = std_dev_90d
            return group

        processed_data = daily_sales.groupby(['City', 'No. Barang'], group_keys=False).apply(process_group).reset_index()
        processed_data.rename(columns={'index': 'Date'}, inplace=True)
        
        # --- [BARU] Logika untuk memilih Z-Score berdasarkan metode ---
        if method == "ABC Bertingkat":
            z_scores = {'A': 1.65, 'B': 1.0, 'C': 0.0, 'D': 0.0}
        elif method == "Uniform":
            z_scores = {'A': 1.0, 'B': 1.0, 'C': 1.0, 'D': 1.0}
        else: # ROP = Min Stock
            z_scores = {'A': 0.0, 'B': 0.0, 'C': 0.0, 'D': 0.0}

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

        abc_classification = avg_sales.groupby('City').apply(classify_abc).reset_index(drop=True)
        final_df = pd.merge(processed_data, abc_classification, on=['City', 'No. Barang'], how='left')

        final_df['Z_Score'] = final_df['Kategori ABC'].map(z_scores).fillna(0).astype(float)
        
        final_df['Safety Stock'] = final_df['Z_Score'] * final_df['std_dev_90d'] * math.sqrt(0.7)
        final_df['Min Stock'] = final_df['WMA'] * (21/30)
        final_df['ROP'] = final_df['Min Stock'] + final_df['Safety Stock']

        final_df = pd.merge(final_df, produk_df, on='No. Barang', how='left')
        final_df = final_df[final_df['Date'].dt.date >= start_date].copy()
        
        final_df['ROP'] = final_df['ROP'].round().astype(int)
        final_df['SO'] = final_df['SO'].astype(int)
        
        return_cols = ['Date', 'City', 'No. Barang', 'Kategori Barang', 'BRAND Barang', 'Nama Barang', 'ROP', 'SO']
        return final_df[return_cols]

    # ... Sisa kode halaman ini (UI & Logika) tidak berubah, hanya memanggil fungsi dengan parameter 'metode_rop'
    # ... (misal: calculate_rop_and_sellout(penjualan, produk_ref, start_date, end_date, metode_rop))

# =====================================================================================
#                           [BARU] HALAMAN ANALISIS ERROR METODE ROP
# =====================================================================================
elif page == "Analisis Error Metode ROP":
    st.title("🎯 Analisis Error Metode ROP")
    st.markdown("Halaman ini membandingkan 3 metode ROP dengan penjualan riil untuk menemukan metode mana yang paling akurat.")
    
    # Prasyarat
    if st.session_state.df_penjualan.empty or st.session_state.produk_ref.empty:
        st.warning("⚠️ Harap muat file **Penjualan** dan **Produk Referensi** di halaman **'Input Data'**.")
        st.stop()

    # Preprocessing Data
    with st.spinner("Menyiapkan data..."):
        penjualan = st.session_state.df_penjualan.copy()
        produk_ref = st.session_state.produk_ref.copy()
        for df in [penjualan, produk_ref]:
            if 'No. Barang' in df.columns:
                df['No. Barang'] = df['No. Barang'].astype(str).str.strip()
        if 'Qty' in penjualan.columns:
            penjualan.rename(columns={'Qty': 'Kuantitas'}, inplace=True)
        penjualan['Nama Dept'] = penjualan.apply(map_nama_dept, axis=1)
        penjualan['City'] = penjualan['Nama Dept'].apply(map_city)
        penjualan = penjualan[penjualan['City'] != 'Others']
        penjualan['Tgl Faktur'] = pd.to_datetime(penjualan['Tgl Faktur'], errors='coerce')
        penjualan.dropna(subset=['Tgl Faktur'], inplace=True)
    
    st.markdown("---")
    st.header("Pilih Rentang Tanggal untuk Analisis Error")
    st.info("Pilih rentang tanggal yang cukup panjang untuk evaluasi. Pastikan data penjualan Anda mencakup 21 hari setelah tanggal akhir yang dipilih untuk perbandingan yang akurat.")
    
    default_end_date = penjualan['Tgl Faktur'].max().date() - timedelta(days=21)
    default_start_date = default_end_date - timedelta(days=29)

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Tanggal Awal", value=default_start_date, key="err_start")
    end_date = col2.date_input("Tanggal Akhir", value=default_end_date, key="err_end")

    if st.button("🚀 Jalankan Analisis Error 🚀"):
        if start_date > end_date:
            st.error("Tanggal Awal tidak boleh melebihi Tanggal Akhir.")
        else:
            with st.spinner("Menjalankan analisis error untuk 3 metode... Ini mungkin memakan waktu."):
                progress_bar = st.progress(0, text="Memulai...")

                # 1. Hitung ROP untuk setiap metode
                rop_abc = calculate_rop_and_sellout(penjualan, produk_ref, start_date, end_date, "ABC Bertingkat")[['Date', 'City', 'No. Barang', 'SO', 'ROP']].rename(columns={'ROP': 'ROP_ABC'})
                progress_bar.progress(33, text="Metode ABC Bertingkat selesai...")
                
                rop_uniform = calculate_rop_and_sellout(penjualan, produk_ref, start_date, end_date, "Uniform")[['Date', 'City', 'No. Barang', 'ROP']].rename(columns={'ROP': 'ROP_Uniform'})
                progress_bar.progress(66, text="Metode Uniform selesai...")

                rop_min = calculate_rop_and_sellout(penjualan, produk_ref, start_date, end_date, "ROP = Min Stock")[['Date', 'City', 'No. Barang', 'ROP']].rename(columns={'ROP': 'ROP_Min_Stock'})
                progress_bar.progress(80, text="Metode Min Stock selesai...")

                # 2. Gabungkan hasil ROP
                merged_rop = pd.merge(rop_abc, rop_uniform, on=['Date', 'City', 'No. Barang'])
                merged_rop = pd.merge(merged_rop, rop_min, on=['Date', 'City', 'No. Barang'])

                # 3. Hitung penjualan riil 21 hari ke depan
                daily_sales_full = merged_rop.set_index('Date').groupby(['City', 'No. Barang'])['SO'].shift(-21).rolling(window=21, min_periods=1).sum().reset_index()
                daily_sales_full = daily_sales_full.rename(columns={'SO': 'Penjualan_Riil_21_Hari'})
                
                # 4. Gabungkan semua data
                final_analysis_df = pd.merge(merged_rop, daily_sales_full, on=['Date', 'City', 'No. Barang'])
                final_analysis_df.dropna(inplace=True)

                # 5. Hitung Error (Selisih Absolut)
                final_analysis_df['Error_ABC'] = (final_analysis_df['Penjualan_Riil_21_Hari'] - final_analysis_df['ROP_ABC']).abs()
                final_analysis_df['Error_Uniform'] = (final_analysis_df['Penjualan_Riil_21_Hari'] - final_analysis_df['ROP_Uniform']).abs()
                final_analysis_df['Error_Min_Stock'] = (final_analysis_df['Penjualan_Riil_21_Hari'] - final_analysis_df['ROP_Min_Stock']).abs()

                st.session_state.error_analysis_result = final_analysis_df
                progress_bar.progress(100, text="Analisis Selesai!")

    if 'error_analysis_result' in st.session_state and st.session_state.error_analysis_result is not None:
        result_df = st.session_state.error_analysis_result
        
        st.markdown("---")
        st.header("🏆 Hasil Perbandingan Metode")

        # Hitung Rata-rata Error (MAE)
        mae_abc = result_df['Error_ABC'].mean()
        mae_uniform = result_df['Error_Uniform'].mean()
        mae_min_stock = result_df['Error_Min_Stock'].mean()

        summary_data = {
            'Metode ROP': ['ABC Bertingkat', 'Uniform', 'ROP = Min Stock'],
            'Rata-Rata Error (MAE)': [mae_abc, mae_uniform, mae_min_stock]
        }
        summary_df = pd.DataFrame(summary_data).set_index('Metode ROP')
        summary_df['Rata-Rata Error (MAE)'] = summary_df['Rata-Rata Error (MAE)'].round(2)
        
        st.subheader("Skor Error Keseluruhan (Semakin Kecil Semakin Baik)")
        st.dataframe(summary_df.style.highlight_min(color='lightgreen', axis=0))

        st.subheader("Visualisasi Perbandingan Error")
        st.bar_chart(summary_df)

        with st.expander("Lihat Rata-Rata Error per Kota"):
            mae_per_city = result_df.groupby('City')[['Error_ABC', 'Error_Uniform', 'Error_Min_Stock']].mean().round(2)
            mae_per_city.columns = ['MAE ABC', 'MAE Uniform', 'MAE Min Stock']
            st.dataframe(mae_per_city.style.highlight_min(color='lightgreen', axis=1))

        with st.expander("Lihat Detail Data Analisis"):
            st.dataframe(result_df)
            excel_data = convert_df_to_excel(result_df)
            st.download_button(
                label="📥 Unduh Detail Analisis Error (Excel)",
                data=excel_data,
                file_name=f"analisis_error_rop_{start_date}_to_{end_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
