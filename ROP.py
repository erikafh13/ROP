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
# FIX: Mengganti parameter 'use_container_width' yang sudah usang
st.sidebar.image("https://i.imgur.com/n0KzG1p.png", width='stretch')
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
if 'error_analysis_result' not in st.session_state:
    st.session_state.error_analysis_result = None
if 'summary_error_result' not in st.session_state:
    st.session_state.summary_error_result = None


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
        folder_penjualan = "1wH9o4dyNfjveScJ_DB2TwT0EDsPe9Zf"
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

def read_produk_file(file_id, sheet_name, skip_rows):
    try:
        fh = download_file_from_gdrive(file_id)
        df = pd.read_excel(fh, sheet_name=sheet_name, skiprows=skip_rows, usecols=[0, 1, 2, 3])
        df.columns = ['No. Barang', 'BRAND Barang', 'Kategori Barang', 'Nama Barang']
        return df
    except Exception as e:
        st.error(f"Gagal membaca file Excel. Pastikan Nama Sheet dan jumlah baris header benar. Detail error: {e}")
        return pd.DataFrame()

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

# --- FUNGSI UTAMA PERHITUNGAN ROP (STRATEGI BARU) ---
@st.cache_data(ttl=3600)
def preprocess_sales_data(_penjualan_df, _produk_df, start_date, end_date):
    """
    Fungsi inti dengan logika perhitungan yang ditulis ulang menggunakan groupby().apply()
    untuk menghindari bug internal pandas pada MultiIndex.
    """
    penjualan_df = _penjualan_df.copy()
    produk_df = _produk_df.copy()

    analysis_start_date = pd.to_datetime(start_date) - pd.DateOffset(days=90)
    extended_end_date = pd.to_datetime(end_date) + pd.DateOffset(days=21)
    date_range_full = pd.date_range(start=analysis_start_date, end=extended_end_date, freq='D')

    daily_sales = penjualan_df.groupby(['Tgl Faktur', 'City', 'No. Barang'])['Kuantitas'].sum().reset_index()
    daily_sales.rename(columns={'Tgl Faktur': 'Date', 'Kuantitas': 'SO'}, inplace=True)
    daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])

    unique_items = daily_sales[['City', 'No. Barang']].drop_duplicates()
    if unique_items.empty:
        return pd.DataFrame()

    date_df = pd.DataFrame({'Date': date_range_full})
    df_full = unique_items.merge(date_df, how='cross')
    df_full = df_full.merge(daily_sales, on=['City', 'No. Barang', 'Date'], how='left').fillna(0)
    df_full = df_full.groupby(['City', 'No. Barang', 'Date']).sum().reset_index()
    
    # Fungsi untuk diterapkan pada setiap grup (setiap produk di setiap kota)
    def calculate_metrics_for_group(group):
        group = group.sort_values('Date')
        group['sales_90d'] = group['SO'].rolling(window=90, min_periods=1).sum()
        group['std_dev_90d'] = group['SO'].rolling(window=90, min_periods=1).std().fillna(0)
        group['ADS'] = group['sales_90d'] / 90
        
        # Kalkulasi penjualan 21 hari ke depan
        reversed_rolling_sum = group['SO'].iloc[::-1].rolling(window=21, min_periods=0).sum().iloc[::-1]
        # FIX: Menghapus .shift() untuk perbandingan error yang benar
        group['Penjualan_Aktual_21_Hari'] = reversed_rolling_sum
        return group

    # Terapkan fungsi ke setiap grup. Ini adalah inti dari perbaikan.
    df_full = df_full.groupby(['City', 'No. Barang'], group_keys=False, include_groups=False).apply(calculate_metrics_for_group)

    # --- Sisa fungsi berjalan seperti biasa ---
    avg_ads = df_full.groupby(['City', 'No. Barang'])['ADS'].mean().reset_index()
    
    if avg_ads.empty:
        abc_classification = pd.DataFrame(columns=['City', 'No. Barang', 'Kategori ABC'])
    else:
        # FIX: Mengganti .apply dengan metode vectorized untuk menghindari FutureWarning
        sorted_ads = avg_ads.sort_values(by=['City', 'ADS'], ascending=[True, False])
        city_totals = sorted_ads.groupby('City')['ADS'].transform('sum')
        sorted_ads['CUM_ADS'] = sorted_ads.groupby('City')['ADS'].cumsum()
        sorted_ads['Cumulative_Perc'] = 100 * sorted_ads['CUM_ADS'] / city_totals.where(city_totals != 0, 1)
        sorted_ads['Kategori ABC'] = pd.cut(sorted_ads['Cumulative_Perc'], bins=[-1, 70, 90, 101], labels=['A', 'B', 'C'], right=True)
        sorted_ads.loc[city_totals == 0, 'Kategori ABC'] = 'D'
        abc_classification = sorted_ads[['City', 'No. Barang', 'Kategori ABC']]

    final_df = pd.merge(df_full, abc_classification, on=['City', 'No. Barang'], how='left')
    final_df = pd.merge(final_df, produk_df, on='No. Barang', how='left')

    final_df = final_df[(final_df['Date'].dt.date >= pd.to_datetime(start_date).date()) & (final_df['Date'].dt.date <= pd.to_datetime(end_date).date())].copy()

    return final_df

def apply_rop_method(df, method):
    LEAD_TIME_DAYS = 21
    FORECAST_PERIOD_DAYS = 90
    df_copy = df.copy()
    if method == "ABC Bertingkat":
        z_scores = {'A': 1.65, 'B': 1.0, 'C': 0.0, 'D': 0.0}
    elif method == "Uniform":
        z_scores = {'A': 1.0, 'B': 1.0, 'C': 1.0, 'D': 1.0}
    else: # ROP = Min Stock
        z_scores = {'A': 0.0, 'B': 0.0, 'C': 0.0, 'D': 0.0}
    if isinstance(df_copy['Kategori ABC'].dtype, pd.CategoricalDtype):
        df_copy['Kategori ABC'] = df_copy['Kategori ABC'].cat.add_categories('D').fillna('D')
    else:
        df_copy['Kategori ABC'] = df_copy['Kategori ABC'].fillna('D')
    df_copy['Z_Score'] = df_copy['Kategori ABC'].map(z_scores)
    df_copy['Prediksi_Stok_Minimal'] = df_copy['ADS'] * LEAD_TIME_DAYS
    lead_time_ratio_std = LEAD_TIME_DAYS / FORECAST_PERIOD_DAYS
    df_copy['Safety_Stock'] = df_copy['Z_Score'] * df_copy['std_dev_90d'] * math.sqrt(lead_time_ratio_std)
    df_copy['ROP'] = df_copy['Prediksi_Stok_Minimal'] + df_copy['Safety_Stock']
    df_copy['ROP'] = df_copy['ROP'].round().astype(int)
    df_copy['SO'] = df_copy['SO'].astype(int)
    return df_copy


# =====================================================================================
#                                    HALAMAN INPUT DATA
# =====================================================================================
if page == "Input Data":
    st.title("📥 Input Data")
    st.markdown("Muat atau muat ulang data yang diperlukan dari Google Drive.")
    if not DRIVE_AVAILABLE:
        st.warning("Tidak dapat melanjutkan karena koneksi ke Google Drive gagal.")
        st.stop()
    st.header("1. Data Penjualan")
    st.info("Tips: Untuk mempercepat pemrosesan, arsipkan file-file penjualan yang sangat lama ke folder lain di Google Drive Anda.")
    with st.spinner("Mencari file penjualan di Google Drive..."):
        penjualan_files_list = list_files_in_folder(drive_service, folder_penjualan)
    if st.button("Muat / Muat Ulang Data Penjualan"):
        if penjualan_files_list:
            progress_bar = st.progress(0, text="Memulai proses pemuatan data...")
            all_dfs = []
            total_files = len(penjualan_files_list)
            try:
                for i, file_info in enumerate(penjualan_files_list):
                    progress_text = f"Memuat file {i + 1}/{total_files}: {file_info['name']}"
                    progress_bar.progress((i + 1) / total_files, text=progress_text)
                    df = download_and_read(file_info['id'], file_info['name'])
                    all_dfs.append(df)
                progress_bar.progress(1.0, text="Menggabungkan semua data...")
                if all_dfs:
                    df_penjualan = pd.concat(all_dfs, ignore_index=True)
                    if 'No. Barang' in df_penjualan.columns:
                        df_penjualan['No. Barang'] = df_penjualan['No. Barang'].astype(str)
                    st.session_state.df_penjualan = df_penjualan
                    progress_bar.empty()
                    st.success("Data penjualan berhasil dimuat ulang.")
                else:
                    progress_bar.empty()
                    st.warning("Tidak ada data untuk digabungkan.")
            except Exception as e:
                progress_bar.empty()
                st.error(f"Terjadi kesalahan saat memuat file: {e}")
                st.exception(e)
        else:
            st.warning("⚠️ Tidak ada file penjualan ditemukan di folder Google Drive.")
    if not st.session_state.df_penjualan.empty:
        df_penjualan_display = st.session_state.df_penjualan.copy()
        st.success(f"✅ Data penjualan telah dimuat ({len(df_penjualan_display)} baris).")
        df_penjualan_display['Tgl Faktur'] = pd.to_datetime(df_penjualan_display['Tgl Faktur'], errors='coerce')
        min_date = df_penjualan_display['Tgl Faktur'].min()
        max_date = df_penjualan_display['Tgl Faktur'].max()
        if pd.notna(min_date) and pd.notna(max_date):
            num_months = len(df_penjualan_display['Tgl Faktur'].dt.to_period('M').unique())
            st.info(f"📅 **Rentang Data:** Dari **{min_date.strftime('%d %B %Y')}** hingga **{max_date.strftime('%d %B %Y')}** ({num_months} bulan data).")
        if 'No. Barang' in df_penjualan_display.columns:
            df_penjualan_display['No. Barang'] = df_penjualan_display['No. Barang'].astype(str)
        st.dataframe(df_penjualan_display)
    st.header("2. Produk Referensi")
    with st.spinner("Mencari file produk di Google Drive..."):
        produk_files_list = list_files_in_folder(drive_service, folder_produk)
    selected_produk_file = st.selectbox(
        "Pilih file Produk dari Google Drive:",
        options=[None] + produk_files_list,
        format_func=lambda x: x['name'] if x else "Pilih file"
    )
    if selected_produk_file:
        with st.form("product_file_config"):
            st.info("Harap konfigurasikan cara membaca file Excel yang dipilih.")
            c1, c2 = st.columns(2)
            sheet_name = c1.text_input("Nama Sheet", value="Sheet1 (2)")
            skip_rows = c2.number_input("Jumlah baris header untuk dilewati", min_value=0, max_value=50, value=6)
            submitted = st.form_submit_button("Muat File Produk")
            if submitted:
                with st.spinner(f"Memuat dan memproses file {selected_produk_file['name']}..."):
                    produk_df = read_produk_file(selected_produk_file['id'], sheet_name, skip_rows)
                    if not produk_df.empty:
                        if 'No. Barang' in produk_df.columns:
                            produk_df['No. Barang'] = produk_df['No. Barang'].astype(str)
                        st.session_state.produk_ref = produk_df
                        st.success(f"File produk referensi '{selected_produk_file['name']}' berhasil dimuat.")
    if not st.session_state.produk_ref.empty:
        st.success(f"✅ Data produk referensi telah dimuat ({len(st.session_state.produk_ref)} baris).")
        st.dataframe(st.session_state.produk_ref.head())
# =====================================================================================
#                                HALAMAN HASIL ANALISA ROP
# =====================================================================================
elif page == "Hasil Analisa ROP":
    st.title("📈 Hasil Analisa ROP & Sell Out")
    st.sidebar.header("🔧 Pengaturan Metode ROP")
    metode_rop = st.sidebar.selectbox(
        "Pilih Metode Perhitungan ROP:",
        ("ABC Bertingkat", "Uniform", "ROP = Min Stock")
    )
    if st.session_state.df_penjualan.empty or st.session_state.produk_ref.empty:
        st.warning("⚠️ Harap muat file **Penjualan** dan **Produk Referensi** di halaman **'Input Data'**.")
        st.stop()
    penjualan = st.session_state.df_penjualan.copy()
    produk_ref = st.session_state.produk_ref.copy()
    for df in [penjualan, produk_ref]:
        if 'No. Barang' in df.columns:
            df['No. Barang'] = df['No. Barang'].astype(str).str.strip()
    if 'Qty' in penjualan.columns and 'Kuantitas' not in penjualan.columns:
        penjualan.rename(columns={'Qty': 'Kuantitas'}, inplace=True)
    elif 'Kuantitas' not in penjualan.columns:
        st.error("Error: Kolom kuantitas ('Qty' atau 'Kuantitas') tidak ditemukan.")
        st.stop()
    penjualan['Nama Dept'] = penjualan.apply(map_nama_dept, axis=1)
    penjualan['City'] = penjualan['Nama Dept'].apply(map_city)
    penjualan = penjualan[penjualan['City'] != 'Others']
    penjualan['Tgl Faktur'] = pd.to_datetime(penjualan['Tgl Faktur'], errors='coerce')
    penjualan.dropna(subset=['Tgl Faktur', 'City'], inplace=True)
    st.markdown("---")
    st.header("Pilih Rentang Tanggal untuk Analisis")
    default_end_date = penjualan['Tgl Faktur'].max().date()
    default_start_date = default_end_date - timedelta(days=6)
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Tanggal Awal", value=default_start_date, key="rop_start")
    end_date = col2.date_input("Tanggal Akhir", value=default_end_date, key="rop_end")
    if st.button("🚀 Jalankan Analisa ROP & SO 🚀"):
        if start_date > end_date:
            st.error("Tanggal Awal tidak boleh melebihi Tanggal Akhir.")
        else:
            try:
                with st.spinner(f"Menjalankan pra-pemrosesan data... Ini mungkin butuh waktu lebih lama."):
                    preprocessed_df = preprocess_sales_data(penjualan, produk_ref, start_date, end_date)
                with st.spinner(f"Menerapkan metode '{metode_rop}'..."):
                    rop_result_df = apply_rop_method(preprocessed_df, metode_rop)
                if not rop_result_df.empty:
                    st.session_state.rop_analysis_result = rop_result_df
                    st.success(f"Analisis berhasil dijalankan!")
                else:
                    st.error("Tidak ada data yang dihasilkan.")
            except Exception as e:
                st.error(f"Terjadi kesalahan saat perhitungan: {e}")
                st.exception(e)
    if st.session_state.rop_analysis_result is not None:
        result_df = st.session_state.rop_analysis_result.copy()
        st.markdown("---"); st.header("🔍 Filter Hasil")
        col_f1, col_f2, col_f3 = st.columns(3)
        kategori_options = sorted(result_df['Kategori Barang'].dropna().unique().astype(str))
        selected_kategori = col_f1.multiselect("Kategori:", kategori_options)
        brand_options = sorted(result_df['BRAND Barang'].dropna().unique().astype(str))
        selected_brand = col_f2.multiselect("Brand:", brand_options)
        product_options = sorted(result_df['Nama Barang'].dropna().unique().astype(str))
        selected_products = col_f3.multiselect("Nama Produk:", product_options)
        if selected_kategori: result_df = result_df[result_df['Kategori Barang'].astype(str).isin(selected_kategori)]
        if selected_brand: result_df = result_df[result_df['BRAND Barang'].astype(str).isin(selected_brand)]
        if selected_products: result_df = result_df[result_df['Nama Barang'].astype(str).isin(selected_products)]
        st.markdown("---")
        result_df['Date'] = result_df['Date'].dt.strftime('%Y-%m-%d')
        pivot_outputs = {}
        st.header("Tabel ROP & SO per Kota")
        unique_cities = [str(city) for city in result_df['City'].dropna().unique()]
        for city in sorted(unique_cities):
            with st.expander(f"📍 Lihat Hasil untuk Kota: {city}", expanded=(city == "Surabaya")):
                city_df = result_df[result_df['City'] == city].copy()
                if not city_df.empty:
                    index_cols = ['No. Barang', 'Nama Barang', 'BRAND Barang', 'Kategori Barang']
                    for col in index_cols:
                        city_df[col] = city_df[col].fillna('Data Tidak Ditemukan')
                    pivot_city = city_df.pivot_table(
                        index=index_cols,
                        columns='Date',
                        values=['ROP', 'SO']
                    ).fillna(0).astype(int)
                    pivot_city.columns = pivot_city.columns.swaplevel(0, 1)
                    pivot_city.sort_index(axis=1, level=0, inplace=True)
                    pivot_outputs[f"ROP_{city.replace(' ', '_')}"] = pivot_city
                    cmap_rop = 'Greens'
                    cmap_so = 'Blues'
                    styled_pivot = pivot_city.style.background_gradient(
                        cmap=cmap_rop,
                        subset=pd.IndexSlice[:, pd.IndexSlice[:, 'ROP']]
                    ).background_gradient(
                        cmap=cmap_so,
                        subset=pd.IndexSlice[:, pd.IndexSlice[:, 'SO']]
                    ).format("{:}")
                    st.write(styled_pivot.to_html(), unsafe_allow_html=True)
                else:
                    st.write("Tidak ada data yang cocok dengan filter.")
        if pivot_outputs:
            st.markdown("---")
            st.header("💾 Unduh Hasil Analisis")
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for sheet_name, df_pivot in pivot_outputs.items():
                    df_pivot.to_excel(writer, sheet_name=sheet_name, index=True)
            st.download_button(
                label="📥 Unduh Semua Hasil ROP & SO (Excel)",
                data=output.getvalue(),
                file_name=f"hasil_rop_so_{start_date}_to_{end_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
# =====================================================================================
#                          HALAMAN ANALISIS ERROR METODE ROP
# =====================================================================================
elif page == "Analisis Error Metode ROP":
    st.title("🎯 Analisis Error Metode ROP")
    st.markdown("Halaman ini membandingkan 3 metode ROP dengan penjualan riil untuk melihat kecenderungan **Overstock** vs **Stockout**.")
    if st.session_state.df_penjualan.empty or st.session_state.produk_ref.empty:
        st.warning("⚠️ Harap muat file **Penjualan** dan **Produk Referensi** di halaman **'Input Data'**.")
        st.stop()
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
    penjualan.dropna(subset=['Tgl Faktur', 'City'], inplace=True)
    st.markdown("---")
    st.header("Pilih Rentang Tanggal untuk Analisis Error")
    st.info("Pilih rentang tanggal evaluasi. Pastikan data penjualan Anda mencakup 21 hari setelah tanggal akhir untuk perbandingan akurat.")
    
    # FIX: Mengatur tanggal default ke Feb-April pada tahun terakhir data
    try:
        max_date_in_data = penjualan['Tgl Faktur'].max()
        latest_year = max_date_in_data.year
        # Batas analisis adalah 21 hari sebelum data terakhir
        analysis_limit_date = max_date_in_data.date() - timedelta(days=21)
        
        default_start_date = datetime(latest_year, 2, 1).date()
        # Gunakan 30 April sebagai akhir, kecuali jika melebihi batas data
        default_end_date = min(datetime(latest_year, 4, 30).date(), analysis_limit_date)
    except:
        # Fallback jika tidak ada data
        default_end_date = datetime.now().date() - timedelta(days=21)
        default_start_date = default_end_date - timedelta(days=29)

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Tanggal Awal", value=default_start_date, key="err_start")
    end_date = col2.date_input("Tanggal Akhir", value=default_end_date, key="err_end")
    
    if st.button("🚀 Jalankan Analisis Error 🚀"):
        if start_date > end_date:
            st.error("Tanggal Awal tidak boleh melebihi Tanggal Akhir.")
        else:
            with st.spinner("Menjalankan analisis... Ini mungkin butuh beberapa saat."):
                progress_bar = st.progress(0, text="Memulai pra-pemrosesan data...")
                preprocessed_df = preprocess_sales_data(penjualan, produk_ref, start_date, end_date)
                progress_bar.progress(40, text="Menerapkan metode ROP...")
                rop_abc = apply_rop_method(preprocessed_df, "ABC Bertingkat")
                rop_uniform = apply_rop_method(preprocessed_df, "Uniform")
                rop_min = apply_rop_method(preprocessed_df, "ROP = Min Stock")
                progress_bar.progress(70, text="Menggabungkan hasil dan menghitung error...")
                analysis_df = preprocessed_df.copy()
                analysis_df['ROP_ABC'] = rop_abc['ROP']
                analysis_df['ROP_Uniform'] = rop_uniform['ROP']
                analysis_df['ROP_Min_Stock'] = rop_min['ROP']
                analysis_df.dropna(subset=['Penjualan_Aktual_21_Hari'], inplace=True)
                analysis_df['Error_ABC'] = analysis_df['ROP_ABC'] - analysis_df['Penjualan_Aktual_21_Hari']
                analysis_df['Error_Uniform'] = analysis_df['ROP_Uniform'] - analysis_df['Penjualan_Aktual_21_Hari']
                analysis_df['Error_Min_Stock'] = analysis_df['ROP_Min_Stock'] - analysis_df['Penjualan_Aktual_21_Hari']
                
                # Simpan hasil untuk digunakan nanti
                st.session_state.error_analysis_result = analysis_df
                
                # Kalkulasi ringkasan keseluruhan
                summary_list = []
                for method in ['ABC', 'Uniform', 'Min_Stock']:
                    error_col = f'Error_{method}'
                    mae = analysis_df[error_col].abs().mean()
                    bias = analysis_df[error_col].mean()
                    stockout_days = (analysis_df[error_col] < 0).sum()
                    summary_list.append({
                        'Metode': method.replace('_', ' '),
                        'MAE': mae,
                        'Rata-rata Error (Bias)': bias,
                        'Jumlah Hari Stockout': stockout_days
                    })
                summary_df = pd.DataFrame(summary_list).set_index('Metode')
                st.session_state.summary_error_result = summary_df
                progress_bar.progress(100, text="Analisis Selesai!")
                
    if 'summary_error_result' in st.session_state and st.session_state.summary_error_result is not None:
        summary_df = st.session_state.summary_error_result
        result_df = st.session_state.error_analysis_result # Ambil kembali data detail
        
        st.markdown("---")
        st.header("🏆 Hasil Perbandingan Metode (Keseluruhan)")
        st.markdown("""
        - **MAE (Mean Absolute Error)**: Rata-rata besaran kesalahan. *Semakin kecil semakin akurat*.
        - **Rata-rata Error (Bias)**: Kecenderungan metode. *Positif berarti cenderung overstock, Negatif berarti cenderung stockout*.
        - **Jumlah Hari Stockout**: Total kejadian prediksi lebih rendah dari aktual. *Semakin kecil semakin baik*.
        """)
        st.dataframe(summary_df.style
            .highlight_min(subset=['MAE', 'Jumlah Hari Stockout'], color='lightgreen')
            .apply(lambda x: ['background-color: lightcoral' if v < 0 else 'background-color: lightblue' for v in x], subset=['Rata-rata Error (Bias)'])
            .format("{:.2f}", subset=['MAE', 'Rata-rata Error (Bias)'])
        )
        
        # --- BAGIAN BARU: Analisis per Kota ---
        st.markdown("---")
        st.header("🏙️ Hasil Perbandingan per Kota")
        
        unique_cities = sorted(result_df['City'].unique())
        for city in unique_cities:
            with st.expander(f"Lihat Hasil untuk Kota: {city}"):
                city_df = result_df[result_df['City'] == city]
                
                summary_list_city = []
                if not city_df.empty:
                    for method in ['ABC', 'Uniform', 'Min_Stock']:
                        error_col = f'Error_{method}'
                        mae = city_df[error_col].abs().mean()
                        bias = city_df[error_col].mean()
                        stockout_days = (city_df[error_col] < 0).sum()
                        summary_list_city.append({
                            'Metode': method.replace('_', ' '),
                            'MAE': mae,
                            'Rata-rata Error (Bias)': bias,
                            'Jumlah Hari Stockout': stockout_days
                        })
                    
                    summary_df_city = pd.DataFrame(summary_list_city).set_index('Metode')
                    
                    st.dataframe(summary_df_city.style
                        .highlight_min(subset=['MAE', 'Jumlah Hari Stockout'], color='lightgreen')
                        .apply(lambda x: ['background-color: lightcoral' if v < 0 else 'background-color: lightblue' for v in x], subset=['Rata-rata Error (Bias)'])
                        .format("{:.2f}", subset=['MAE', 'Rata-rata Error (Bias)'])
                    )
                else:
                    st.write("Tidak ada data untuk kota ini.")

