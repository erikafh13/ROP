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
if 'error_analysis_result' not in st.session_state:
    st.session_state.error_analysis_result = None

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
    
# --- [REFAKTORISASI TOTAL] FUNGSI UTAMA UNTUK EFISIENSI MEMORI ---
@st.cache_data(ttl=3600)
def preprocess_sales_data(_penjualan_df, _produk_df, start_date, end_date):
    """
    Fungsi inti yang direvisi total untuk efisiensi memori (vectorization).
    Mampu menangani rentang tanggal yang jauh lebih panjang.
    """
    penjualan_df = _penjualan_df.copy()
    produk_df = _produk_df.copy()

    # Tentukan rentang tanggal penuh yang diperlukan untuk semua kalkulasi
    analysis_start_date = pd.to_datetime(start_date) - pd.DateOffset(days=90)
    extended_end_date = pd.to_datetime(end_date) + pd.DateOffset(days=21)
    date_range_full = pd.date_range(start=analysis_start_date, end=extended_end_date, freq='D')

    # Agregasi penjualan harian
    daily_sales = penjualan_df.groupby(['Tgl Faktur', 'City', 'No. Barang'])['Kuantitas'].sum().reset_index()
    daily_sales.rename(columns={'Tgl Faktur': 'Date', 'Kuantitas': 'SO'}, inplace=True)
    daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])

    # --- Langkah 1: Buat 'grid' lengkap untuk semua kombinasi ---
    unique_items = daily_sales[['City', 'No. Barang']].drop_duplicates()
    index_product = pd.MultiIndex.from_product(
        [unique_items['City'].unique(), unique_items['No. Barang'].unique(), date_range_full],
        names=['City', 'No. Barang', 'Date']
    )
    # Gabungkan grid dengan data penjualan riil
    df_full = pd.DataFrame(index=index_product).join(
        daily_sales.set_index(['City', 'No. Barang', 'Date'])
    ).fillna(0).reset_index()

    # --- Langkah 2: Lakukan kalkulasi rolling secara ter-vectorisasi ---
    # Grup berdasarkan produk-kota
    grouped = df_full.set_index('Date').groupby(['City', 'No. Barang'])['SO']
    
    # Hitung rolling sums
    sales_30d = grouped.rolling(window=30, min_periods=1).sum().reset_index(level=[0,1], drop=True)
    sales_60d = grouped.rolling(window=60, min_periods=1).sum().reset_index(level=[0,1], drop=True)
    sales_90d = grouped.rolling(window=90, min_periods=1).sum().reset_index(level=[0,1], drop=True)
    std_dev_90d = grouped.rolling(window=90, min_periods=1).std().reset_index(level=[0,1], drop=True).fillna(0)
    
    # Masukkan hasil kalkulasi ke dataframe utama
    df_full = df_full.set_index(['City', 'No. Barang', 'Date'])
    df_full['sales_30d'] = sales_30d
    df_full['sales_60d'] = sales_60d
    df_full['sales_90d'] = sales_90d
    df_full['std_dev_90d'] = std_dev_90d

    # --- Langkah 3: Hitung WMA dan Penjualan Riil ---
    df_full['WMA'] = (df_full['sales_30d'] * 0.5) + \
                     ((df_full['sales_60d'] - df_full['sales_30d']) * 0.3) + \
                     ((df_full['sales_90d'] - df_full['sales_60d']) * 0.2)

    # Kalkulasi forward-looking sum yang efisien dengan transform
    forward_sum_calculator = lambda x: x.iloc[::-1].rolling(window=21, min_periods=0).sum().iloc[::-1].shift(-21)
    df_full['Penjualan_Riil_21_Hari'] = df_full.groupby(['City', 'No. Barang'])['SO'].transform(forward_sum_calculator)
    
    df_full.reset_index(inplace=True)

    # --- Langkah 4: Klasifikasi ABC ---
    avg_sales = df_full.groupby(['City', 'No. Barang'])['WMA'].mean().reset_index()
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
    
    # --- Langkah 5: Finalisasi ---
    final_df = pd.merge(df_full, abc_classification, on=['City', 'No. Barang'], how='left')
    final_df = pd.merge(final_df, produk_df, on='No. Barang', how='left')
    
    # Filter sesuai rentang tanggal yang diminta pengguna di akhir
    final_df = final_df[(final_df['Date'].dt.date >= pd.to_datetime(start_date).date()) & (final_df['Date'].dt.date <= pd.to_datetime(end_date).date())].copy()
    
    return final_df


# 2. FUNGSI RINGAN UNTUK MENERAPKAN METODE ROP
def apply_rop_method(df, method):
    LEAD_TIME_DAYS = 21
    FORECAST_PERIOD_DAYS = 90 # WMA dan Std Dev berbasis 90 hari

    df_copy = df.copy()

    if method == "ABC Bertingkat":
        z_scores = {'A': 1.65, 'B': 1.0, 'C': 0.0, 'D': 0.0}
    elif method == "Uniform":
        z_scores = {'A': 1.0, 'B': 1.0, 'C': 1.0, 'D': 1.0}
    else: # ROP = Min Stock
        z_scores = {'A': 0.0, 'B': 0.0, 'C': 0.0, 'D': 0.0}

    df_copy['Kategori ABC'] = df_copy['Kategori ABC'].cat.add_categories('D').fillna('D')
    df_copy['Z_Score'] = df_copy['Kategori ABC'].map(z_scores)
    
    lead_time_ratio_demand = LEAD_TIME_DAYS / 30 # Min stock berbasis rerata 30 hari (sesuai logika WMA)
    lead_time_ratio_std = LEAD_TIME_DAYS / FORECAST_PERIOD_DAYS

    df_copy['Safety Stock'] = df_copy['Z_Score'] * df_copy['std_dev_90d'] * math.sqrt(lead_time_ratio_std)
    df_copy['Min Stock'] = df_copy['WMA'] * lead_time_ratio_demand
    
    df_copy['ROP'] = (df_copy['Min Stock'] + df_copy['Safety Stock']).round().astype(int)
    df_copy['SO'] = df_copy['SO'].astype(int)

    return_cols = ['Date', 'City', 'No. Barang', 'Kategori Barang', 'BRAND Barang', 'Nama Barang', 'ROP', 'SO', 'Penjualan_Riil_21_Hari']
    return df_copy[return_cols]

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
    with st.spinner("Mencari file penjualan di Google Drive..."):
        penjualan_files_list = list_files_in_folder(drive_service, folder_penjualan)
    if st.button("Muat / Muat Ulang Data Penjualan"):
        if penjualan_files_list:
            with st.spinner("Menggabungkan semua file penjualan..."):
                df_penjualan = pd.concat([download_and_read(f['id'], f['name']) for f in penjualan_files_list], ignore_index=True)
                if 'No. Barang' in df_penjualan.columns:
                    df_penjualan['No. Barang'] = df_penjualan['No. Barang'].astype(str)
                st.session_state.df_penjualan = df_penjualan
                st.success("Data penjualan berhasil dimuat ulang.")
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

    # Preprocessing data penjualan (bagian yang cepat)
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
    penjualan.dropna(subset=['Tgl Faktur'], inplace=True)
    
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
                with st.spinner(f"Menjalankan pra-pemrosesan data... Ini mungkin butuh waktu untuk rentang panjang."):
                    preprocessed_df = preprocess_sales_data(penjualan, produk_ref, start_date, end_date)
                
                with st.spinner(f"Menerapkan metode '{metode_rop}'..."):
                    rop_result_df = apply_rop_method(preprocessed_df, metode_rop)

                if not rop_result_df.empty:
                    st.session_state.rop_analysis_result = rop_result_df
                    st.success(f"Analisis berhasil dijalankan!")
                else:
                    st.error("Tidak ada data yang dihasilkan untuk rentang yang dipilih.")
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
    st.markdown("Halaman ini membandingkan 3 metode ROP dengan penjualan riil untuk menemukan metode mana yang paling akurat.")
    
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
    penjualan.dropna(subset=['Tgl Faktur'], inplace=True)
    
    st.markdown("---")
    st.header("Pilih Rentang Tanggal untuk Analisis Error")
    st.info("Dengan metode yang lebih efisien, Anda kini bisa mencoba rentang yang lebih panjang. Namun, tetap perhatikan batas wajar sumber daya server.")
    
    default_end_date = penjualan['Tgl Faktur'].max().date() - timedelta(days=21)
    default_start_date = default_end_date - timedelta(days=29)

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Tanggal Awal", value=default_start_date, key="err_start")
    end_date = col2.date_input("Tanggal Akhir", value=default_end_date, key="err_end")

    if st.button("🚀 Jalankan Analisis Error 🚀"):
        if start_date > end_date:
            st.error("Tanggal Awal tidak boleh melebihi Tanggal Akhir.")
        else:
            progress_bar = st.progress(0, text="Memulai...")
            
            progress_bar.progress(10, text="Menjalankan pra-pemrosesan data (langkah terberat)...")
            preprocessed_df = preprocess_sales_data(penjualan, produk_ref, start_date, end_date)
            
            progress_bar.progress(50, text="Menerapkan metode ROP...")
            rop_abc = apply_rop_method(preprocessed_df, "ABC Bertingkat")
            rop_uniform = apply_rop_method(preprocessed_df, "Uniform")
            rop_min = apply_rop_method(preprocessed_df, "ROP = Min Stock")
            
            progress_bar.progress(80, text="Menggabungkan hasil dan menghitung error...")
            final_analysis_df = rop_abc.rename(columns={'ROP': 'ROP_ABC'})
            final_analysis_df['ROP_Uniform'] = rop_uniform['ROP']
            final_analysis_df['ROP_Min_Stock'] = rop_min['ROP']
            
            final_analysis_df.dropna(subset=['Penjualan_Riil_21_Hari'], inplace=True)
            
            final_analysis_df['Error_ABC'] = (final_analysis_df['Penjualan_Riil_21_Hari'] - final_analysis_df['ROP_ABC']).abs()
            final_analysis_df['Error_Uniform'] = (final_analysis_df['Penjualan_Riil_21_Hari'] - final_analysis_df['ROP_Uniform']).abs()
            final_analysis_df['Error_Min_Stock'] = (final_analysis_df['Penjualan_Riil_21_Hari'] - final_analysis_df['ROP_Min_Stock']).abs()

            st.session_state.error_analysis_result = final_analysis_df
            progress_bar.progress(100, text="Analisis Selesai!")

    if 'error_analysis_result' in st.session_state and st.session_state.error_analysis_result is not None:
        result_df = st.session_state.error_analysis_result
        
        st.markdown("---")
        st.header("🏆 Hasil Perbandingan Metode")

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
