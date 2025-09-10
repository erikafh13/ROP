import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import math
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaIoBaseUpload
import io
import os
import re
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Konfigurasi awal halaman Streamlit
st.set_page_config(layout="wide", page_title="Analisis Stock & ROP")

# --- SIDEBAR ---
st.sidebar.image("https://i.imgur.com/n0KzG1p.png", use_container_width=True)
st.sidebar.title("Analisis Stock dan ROP")

page = st.sidebar.radio(
    "Menu Navigasi:",
    ("Input Data", "Hasil Analisa ROP"),
    help="Pilih halaman untuk ditampilkan."
)
st.sidebar.markdown("---")

# --- Inisialisasi Session State ---
if 'df_penjualan' not in st.session_state:
    st.session_state.df_penjualan = pd.DataFrame()
if 'produk_ref' not in st.session_state:
    st.session_state.produk_ref = pd.DataFrame()
if 'df_stock' not in st.session_state:
    st.session_state.df_stock = pd.DataFrame()
if 'stock_filename' not in st.session_state:
    st.session_state.stock_filename = ""
if 'rop_analysis_result' not in st.session_state:
    st.session_state.rop_analysis_result = None
if 'abc_analysis_result' not in st.session_state:
    st.session_state.abc_analysis_result = None


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
        folder_penjualan = "1Okgw8qHVM8HyBwnTUFHbmYkNKqCcswNZ"
        folder_produk = "1UdGbFzZ2Wv83YZLNwdU-rgY-LXlczsFv"
        folder_stock = "1PMeH_wvgRUnyiZyZ_wrmKAATX9JyWzq_"
        folder_hasil_analisis = "1TE4a8IegbWDKoVeLPG_oCbuU-qnhd1jE"
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

def read_stock_file(file_id):
    fh = download_file_from_gdrive(file_id)
    df = pd.read_excel(fh, sheet_name="Sheet1", skiprows=9, header=None)
    header = ['No. Barang', 'Keterangan Barang', 'A - ITC', 'AT - TRANSIT ITC', 'B', 'BT - TRANSIT JKT', 'C', 'C6', 'CT - TRANSIT PUSAT', 'D - SMG', 'DT - TRANSIT SMG', 'E - JOG', 'ET - TRANSIT JOG', 'F - MLG', 'FT - TRANSIT MLG', 'H - BALI', 'HT - TRANSIT BALI', 'X', 'Y - SBY', 'Y3 - Display Y', 'YT - TRANSIT Y']
    df.columns = header[:len(df.columns)]
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
    if nama_dept in ['A - ITC', 'A - RETAIL', 'C - PUSAT', 'G - PROJECT']: return 'Surabaya'
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
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()
    return processed_data


# =====================================================================================
#                                       ROUTING HALAMAN
# =====================================================================================

if page == "Input Data":
    st.title("📥 Input Data")
    st.markdown("Muat atau muat ulang data yang diperlukan dari Google Drive.")

    if not DRIVE_AVAILABLE:
        st.warning("Tidak dapat melanjutkan karena koneksi ke Google Drive gagal. Periksa log di sidebar.")
        st.stop()

    st.header("1. Data Penjualan")
    with st.spinner("Mencari file penjualan di Google Drive..."):
        penjualan_files_list = list_files_in_folder(drive_service, folder_penjualan)
    if st.button("Muat / Muat Ulang Data Penjualan"):
        if penjualan_files_list:
            with st.spinner("Menggabungkan semua file penjualan..."):
                df_penjualan = pd.concat([download_and_read(f['id'], f['name']) for f in penjualan_files_list], ignore_index=True)
                st.session_state.df_penjualan = df_penjualan
                st.success("Data penjualan berhasil dimuat ulang.")
        else:
            st.warning("⚠️ Tidak ada file penjualan ditemukan di folder Google Drive.")

    if not st.session_state.df_penjualan.empty:
        st.success(f"✅ Data penjualan telah dimuat.")
        st.dataframe(st.session_state.df_penjualan)
        
        excel_data = convert_df_to_excel(st.session_state.df_penjualan)
        st.download_button(
            label="📥 Unduh Data Penjualan Gabungan (Excel)",
            data=excel_data,
            file_name="data_penjualan_gabungan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.header("2. Produk Referensi")
    with st.spinner("Mencari file produk di Google Drive..."):
        produk_files_list = list_files_in_folder(drive_service, folder_produk)
    selected_produk_file = st.selectbox(
        "Pilih file Produk dari Google Drive (pilih 1 file):",
        options=[None] + produk_files_list,
        format_func=lambda x: x['name'] if x else "Pilih file"
    )
    if selected_produk_file:
        with st.spinner(f"Memuat file {selected_produk_file['name']}..."):
            st.session_state.produk_ref = read_produk_file(selected_produk_file['id'])
            st.success(f"File produk referensi '{selected_produk_file['name']}' berhasil dimuat.")
    if not st.session_state.produk_ref.empty:
        st.dataframe(st.session_state.produk_ref.head())

    # Data Stock tidak lagi diperlukan untuk perhitungan ROP dinamis
    # Namun, kita biarkan di sini jika diperlukan untuk analisis lain di masa depan
    st.header("3. Data Stock (Opsional)")
    with st.spinner("Mencari file stock di Google Drive..."):
        stock_files_list = list_files_in_folder(drive_service, folder_stock)
    selected_stock_file = st.selectbox(
        "Pilih file Stock dari Google Drive (pilih 1 file):",
        options=[None] + stock_files_list,
        format_func=lambda x: x['name'] if x else "Pilih file"
    )
    if selected_stock_file:
        with st.spinner(f"Memuat file {selected_stock_file['name']}..."):
            st.session_state.df_stock = read_stock_file(selected_stock_file['id'])
            st.session_state.stock_filename = selected_stock_file['name']
            st.success(f"File stock '{selected_stock_file['name']}' berhasil dimuat.")
    if not st.session_state.df_stock.empty:
        st.dataframe(st.session_state.df_stock.head())


elif page == "Hasil Analisa ROP":
    st.title("📈 Hasil Analisa Reorder Point (ROP)")

    # --- FUNGSI-FUNGSI SPESIFIK ANALISA ROP ---
    def calculate_daily_wma(group, end_date):
        end_date = pd.to_datetime(end_date)
        range1_start = end_date - pd.DateOffset(days=29)
        range2_start = end_date - pd.DateOffset(days=59); range2_end = end_date - pd.DateOffset(days=30)
        range3_start = end_date - pd.DateOffset(days=89); range3_end = end_date - pd.DateOffset(days=60)
        sales_last_30_days = group[(group['Tgl Faktur'] >= range1_start) & (group['Tgl Faktur'] <= end_date)]['Kuantitas'].sum()
        sales_31_to_60_days = group[(group['Tgl Faktur'] >= range2_start) & (group['Tgl Faktur'] <= range2_end)]['Kuantitas'].sum()
        sales_61_to_90_days = group[(group['Tgl Faktur'] >= range3_start) & (group['Tgl Faktur'] <= range3_end)]['Kuantitas'].sum()
        wma = (sales_last_30_days * 0.5) + (sales_31_to_60_days * 0.3) + (sales_61_to_90_days * 0.2)
        return wma

    def classify_abc(df_group):
        group = df_group.sort_values(by='Total Kuantitas', ascending=False).reset_index(drop=True)
        terjual = group[group['Total Kuantitas'] > 0].copy()
        tidak_terjual = group[group['Total Kuantitas'] <= 0].copy()
        total_kuantitas = terjual['Total Kuantitas'].sum()
        if total_kuantitas > 0:
            terjual['% kontribusi'] = 100 * terjual['Total Kuantitas'] / total_kuantitas
            terjual['% Kumulatif'] = terjual['% kontribusi'].cumsum()
            terjual['Kategori ABC'] = terjual['% Kumulatif'].apply(lambda x: 'A' if x <= 70 else ('B' if x <= 90 else 'C'))
        else:
            terjual['% kontribusi'] = 0; terjual['% Kumulatif'] = 0; terjual['Kategori ABC'] = 'D'
        tidak_terjual['% kontribusi'] = 0; tidak_terjual['% Kumulatif'] = 100; tidak_terjual['Kategori ABC'] = 'D'
        return pd.concat([terjual, tidak_terjual], ignore_index=True)

    def remove_outliers(data, threshold=2):
        if not isinstance(data, list) or len(data) < 2: return data
        avg = np.mean(data); std = np.std(data)
        if std == 0: return data
        return [x for x in data if abs(x - avg) <= threshold * std]

    def get_z_score(category, volatility):
        base_z = {'A': 1.65, 'B': 1.0, 'C': 0.0, 'D': 0.0}.get(category, 0.0)
        if volatility > 1.5: return base_z + 0.2
        elif volatility < 0.5: return base_z - 0.2
        return base_z

    def calculate_safety_stock_from_series(penjualan_bulanan, category, lead_time=0.7):
        clean_data = remove_outliers(penjualan_bulanan)
        if len(clean_data) < 2: return 0
        std_dev = np.std(clean_data); mean = np.mean(clean_data)
        if mean == 0: return 0
        volatility = std_dev / mean
        z = get_z_score(category, volatility)
        return round(z * std_dev * math.sqrt(lead_time), 2)

    def calculate_min_stock(avg_wma):
        return avg_wma * (21/30)

    def calculate_rop(min_stock, safety_stock): return min_stock + safety_stock

    # --- FUNGSI UTAMA PERHITUNGAN ROP UNTUK SATU TANGGAL ---
    def calculate_rop_for_date(target_date, penjualan_df, produk_df):
        target_date_dt = pd.to_datetime(target_date)
        wma_start_date = target_date_dt - pd.DateOffset(days=89)
        penjualan_for_wma = penjualan_df[(penjualan_df['Tgl Faktur'] >= wma_start_date) & (penjualan_df['Tgl Faktur'] <= target_date_dt)]

        if penjualan_for_wma.empty:
            return pd.DataFrame() # Kembalikan DataFrame kosong jika tidak ada penjualan

        wma_grouped = penjualan_for_wma.groupby(['City', 'No. Barang']).apply(calculate_daily_wma, end_date=target_date).reset_index(name='AVG WMA')
        barang_list = produk_df[['No. Barang', 'Kategori Barang', 'BRAND Barang', 'Nama Barang']].drop_duplicates()
        city_list = penjualan_df['City'].unique()
        kombinasi = pd.MultiIndex.from_product([city_list, barang_list['No. Barang']], names=['City', 'No. Barang']).to_frame(index=False)
        full_data = pd.merge(kombinasi, barang_list, on='No. Barang', how='left')
        full_data = pd.merge(full_data, wma_grouped, on=['City', 'No. Barang'], how='left').fillna(0)

        penjualan_for_wma['Bulan'] = penjualan_for_wma['Tgl Faktur'].dt.to_period('M')
        monthly_sales = penjualan_for_wma.groupby(['City', 'No. Barang', 'Bulan'])['Kuantitas'].sum().unstack(fill_value=0).reset_index()
        full_data = pd.merge(full_data, monthly_sales, on=['City', 'No. Barang'], how='left').fillna(0)
        
        full_data['Total Kuantitas'] = full_data['AVG WMA']
        final_result = full_data.groupby('City', group_keys=False).apply(classify_abc).reset_index(drop=True)
        
        bulan_columns = [col for col in final_result.columns if isinstance(col, pd.Period)]
        final_result['Safety Stock'] = final_result.apply(lambda row: calculate_safety_stock_from_series(row[bulan_columns].tolist(), row['Kategori ABC']), axis=1)
        final_result['Min Stock'] = final_result['AVG WMA'].apply(calculate_min_stock)
        final_result['ROP'] = final_result.apply(lambda row: calculate_rop(row['Min Stock'], row['Safety Stock']), axis=1)
        
        # Tambahkan kolom tanggal
        final_result['Date'] = target_date_dt.date()

        # Pilih kolom yang relevan
        return final_result[['Date', 'City', 'No. Barang', 'Kategori Barang', 'BRAND Barang', 'Nama Barang', 'ROP']]

    # --- Cek prasyarat data ---
    if st.session_state.df_penjualan.empty or st.session_state.produk_ref.empty:
        st.warning("⚠️ Harap muat file **Penjualan** dan **Produk Referensi** di halaman **'Input Data'** terlebih dahulu.")
        st.stop()

    # --- Preprocessing Data ---
    penjualan = st.session_state.df_penjualan.copy()
    produk_ref = st.session_state.produk_ref.copy()

    for df in [penjualan, produk_ref]:
        if 'No. Barang' in df.columns:
            df['No. Barang'] = df['No. Barang'].astype(str).str.strip()

    penjualan.rename(columns={'Qty': 'Kuantitas'}, inplace=True, errors='ignore')
    penjualan['Nama Dept'] = penjualan.apply(map_nama_dept, axis=1)
    penjualan['City'] = penjualan['Nama Dept'].apply(map_city)
    produk_ref.rename(columns={'Keterangan Barang': 'Nama Barang'}, inplace=True, errors='ignore')
    penjualan['Tgl Faktur'] = pd.to_datetime(penjualan['Tgl Faktur'], errors='coerce')
    penjualan.dropna(subset=['Tgl Faktur'], inplace=True)
    
    st.markdown("---")

    # --- UI untuk memilih rentang tanggal ---
    st.header("Pilih Rentang Tanggal untuk Perhitungan ROP")
    default_end_date = penjualan['Tgl Faktur'].max().date()
    default_start_date = default_end_date - timedelta(days=6)

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Tanggal Awal", value=default_start_date, key="rop_start")
    end_date = col2.date_input("Tanggal Akhir", value=default_end_date, key="rop_end")

    if st.button("Jalankan Analisa ROP"):
        if start_date > end_date:
            st.error("Tanggal Awal tidak boleh melebihi Tanggal Akhir.")
        else:
            date_range = pd.date_range(start_date, end_date)
            
            # Peringatan jika rentang tanggal terlalu panjang
            if len(date_range) > 31:
                st.warning(f"Anda memilih rentang {len(date_range)} hari. Perhitungan mungkin memakan waktu lama.")

            all_rop_results = []
            progress_bar = st.progress(0, text="Memulai perhitungan ROP harian...")

            # Loop perhitungan untuk setiap hari dalam rentang
            for i, current_date in enumerate(date_range):
                progress_text = f"Menghitung ROP untuk tanggal: {current_date.strftime('%Y-%m-%d')} ({i+1}/{len(date_range)})"
                progress_bar.progress((i + 1) / len(date_range), text=progress_text)
                
                daily_result = calculate_rop_for_date(current_date, penjualan, produk_ref)
                if not daily_result.empty:
                    all_rop_results.append(daily_result)
            
            progress_bar.empty()

            if all_rop_results:
                final_rop_df = pd.concat(all_rop_results, ignore_index=True)
                # Pembulatan nilai ROP ke integer
                final_rop_df['ROP'] = final_rop_df['ROP'].round(0).astype(int)
                st.session_state.rop_analysis_result = final_rop_df
                st.success(f"Analisis ROP untuk {len(date_range)} hari berhasil dijalankan!")
            else:
                st.error("Tidak ditemukan data penjualan yang cukup untuk melakukan analisis pada rentang tanggal yang dipilih.")
                st.session_state.rop_analysis_result = None

    # --- Tampilkan hasil jika ada ---
    if st.session_state.rop_analysis_result is not None:
        result_df = st.session_state.rop_analysis_result.copy()
        result_df = result_df[result_df['City'] != 'Others']
        
        st.markdown("---")
        st.header("Filter Produk")
        col_f1, col_f2, col_f3 = st.columns(3)
        kategori_options = sorted(result_df['Kategori Barang'].dropna().unique().astype(str))
        selected_kategori = col_f1.multiselect("Kategori:", kategori_options)
        
        brand_options = sorted(result_df['BRAND Barang'].dropna().unique().astype(str))
        selected_brand = col_f2.multiselect("Brand:", brand_options)
        
        product_options = sorted(result_df['Nama Barang'].dropna().unique().astype(str))
        selected_products = col_f3.multiselect("Nama Produk:", product_options)
        
        # Terapkan filter
        if selected_kategori: result_df = result_df[result_df['Kategori Barang'].astype(str).isin(selected_kategori)]
        if selected_brand: result_df = result_df[result_df['BRAND Barang'].astype(str).isin(selected_brand)]
        if selected_products: result_df = result_df[result_df['Nama Barang'].astype(str).isin(selected_products)]
        
        st.markdown("---")
        
        # --- Tabel per Kota ---
        st.header("Tabel ROP per Kota")
        for city in sorted(result_df['City'].unique()):
            with st.expander(f"📍 Lihat Hasil ROP untuk Kota: {city}"):
                city_df = result_df[result_df['City'] == city]
                if city_df.empty:
                    st.write("Tidak ada data yang cocok dengan filter yang dipilih.")
                    continue
                
                # Pivot tabel untuk format yang diminta
                pivot_city = city_df.pivot_table(
                    index=['No. Barang', 'Nama Barang', 'BRAND Barang', 'Kategori Barang'],
                    columns='Date',
                    values='ROP'
                ).fillna(0).astype(int)
                
                st.dataframe(pivot_city, use_container_width=True)

        # --- Tabel Gabungan Semua Kota ---
        st.header("📊 Tabel Gabungan ROP Seluruh Kota")
        if result_df.empty:
            st.warning("Tidak ada data untuk ditampilkan pada tabel gabungan berdasarkan filter yang dipilih.")
        else:
            with st.spinner("Membuat tabel pivot gabungan..."):
                # Agregasi total ROP per barang per tanggal
                total_rop = result_df.groupby(['Date', 'No. Barang', 'Nama Barang', 'BRAND Barang', 'Kategori Barang'])['ROP'].sum().reset_index()

                # Pivot tabel gabungan
                pivot_all = total_rop.pivot_table(
                    index=['No. Barang', 'Nama Barang', 'BRAND Barang', 'Kategori Barang'],
                    columns='Date',
                    values='ROP'
                ).fillna(0).astype(int)
                
                st.dataframe(pivot_all, use_container_width=True)

        # --- Fungsi Unduh ---
        st.header("💾 Unduh Hasil Analisis ROP")
        output_rop = BytesIO()
        with pd.ExcelWriter(output_rop, engine='openpyxl') as writer:
            if 'pivot_all' in locals() and not pivot_all.empty:
                pivot_all.to_excel(writer, sheet_name="All Cities ROP")
            
            for city in sorted(result_df['City'].unique()):
                city_df_to_save = result_df[result_df['City'] == city]
                if not city_df_to_save.empty:
                    pivot_city_to_save = city_df_to_save.pivot_table(
                        index=['No. Barang', 'Nama Barang', 'BRAND Barang', 'Kategori Barang'],
                        columns='Date',
                        values='ROP'
                    ).fillna(0).astype(int)
                    # Nama sheet tidak boleh lebih dari 31 karakter
                    sheet_name = city.replace(" ", "")[:31]
                    pivot_city_to_save.to_excel(writer, sheet_name=f"ROP_{sheet_name}")

        st.download_button(
            "📥 Unduh Hasil Analisis ROP (Excel)",
            data=output_rop.getvalue(),
            file_name=f"Hasil_Analisis_ROP_{start_date}_sd_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
