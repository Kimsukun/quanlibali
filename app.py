import streamlit as st
import pandas as pd
import pdfplumber
import re
from datetime import datetime
import time
import base64
import hashlib
import sqlite3
import os
import io
import sys
import subprocess
import random
import string
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PIL import ImageEnhance
from typing import Any, List, Optional, Union, Literal, overload, Dict

# --- AUTO INSTALL FUNCTION ---
def auto_install(package):
    """Tự động cài đặt thư viện vào đúng môi trường Python đang chạy"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return True
    except: return False

try:
    import google.generativeai as genai
    # Kiểm tra version, nếu cũ quá thì force update (tùy chọn, nhưng nên làm)
    import importlib.metadata
    ver = importlib.metadata.version("google-generativeai")
    if ver < "0.7.0": raise ImportError
except ImportError:
    # Thêm --upgrade để cài bản mới nhất
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "google-generativeai"])
    import google.generativeai as genai
    
import json

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload
except ImportError:
    auto_install("gspread")
    auto_install("google-api-python-client")
    import gspread
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload

# --- OCR CONFIGURATION ---
try:
    import pytesseract
    # CODE MỚI (Tự động nhận diện môi trường)
    if os.path.exists(r'C:\Program Files\Tesseract-OCR\tesseract.exe'):
        # Chạy trên máy tính Windows cá nhân
        pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    else:
        # Chạy trên Streamlit Cloud (Linux) - Không cần set path, nó tự tìm
        pass
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    pytesseract = None
except Exception: # Bắt các lỗi khác, ví dụ như không tìm thấy Tesseract
    HAS_OCR = False
    pytesseract = None

# --- EXCEL LIBS CHECK ---
try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    if auto_install("openpyxl"):
        try: import openpyxl; HAS_OPENPYXL = True
        except: HAS_OPENPYXL = False
    else: HAS_OPENPYXL = False

try:
    import xlsxwriter
    HAS_XLSXWRITER = True
except ImportError:
    if auto_install("xlsxwriter"):
        try: import xlsxwriter; HAS_XLSXWRITER = True
        except: HAS_XLSXWRITER = False
    else: HAS_XLSXWRITER = False

# --- CV & NUMPY LIBS CHECK ---
cv2: Any = None
np: Any = None
HAS_CV = False # Default to False
try:
    import cv2
    import numpy as np
    HAS_CV = True
except ImportError:
    if auto_install("opencv-python-headless") and auto_install("numpy"):
        try: import cv2; import numpy as np; HAS_CV = True # type: ignore
        except: HAS_CV = False
    else:
        HAS_CV = False

# ==========================================
# 1. CẤU HÌNH TRANG & KHỞI TẠO MÔI TRƯỜNG
# ==========================================
st.set_page_config(
    page_title="Quản Lý Hóa Đơn Pro ", 
    page_icon="🌸", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CẤU HÌNH GOOGLE (Đã cập nhật theo thông tin của bạn) ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Tên file chìa khóa (Hãy đổi tên file bạn tải về thành tên này)
SERVICE_ACCOUNT_FILE = 'service_account.json'

# ID Google Drive (Nơi lưu ảnh/pdf)
# Link: https://drive.google.com/drive/folders/1PMCKIUirYwbacu0evnRyuF0xSq-bQtBv?usp=drive_link
DRIVE_FOLDER_ID = '1PMCKIUirYwbacu0evnRyuF0xSq-bQtBv'

# ID Google Sheet (Lấy từ link bạn gửi)
# Link: https://docs.google.com/spreadsheets/d/1coeIPogjKEJSKv1hW1dFBrSAwF6V7c-tkVCZPuPQjoc/edit?gid=0#gid=0
SPREADSHEET_ID = '1coeIPogjKEJSKv1hW1dFBrSAwF6V7c-tkVCZPuPQjoc'

def get_gspread_client():
    # Kiểm tra xem đang chạy trên Cloud (dùng secrets) hay Local (dùng file json)
    if "gcp_service_account" in st.secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    client = gspread.authorize(creds)
    return client

def get_drive_service():
    if "gcp_service_account" in st.secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    service = build('drive', 'v3', credentials=creds)
    return service

# --- CÁC HÀM XỬ LÝ DỮ LIỆU MỚI (Thay thế SQL) ---

def load_table(table_name):
    """Đọc dữ liệu từ Local SQLite (Thay thế Google Sheet)"""
    conn = get_connection()
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df
    except Exception as e:
        print(f"Lỗi đọc bảng {table_name}: {e}")
        return pd.DataFrame()

def add_row_to_table(table_name, row_dict):
    """Thêm dòng mới vào Local SQLite VÀ Google Sheet"""
    # 1. Ghi vào SQLite (Local)
    conn = get_connection()
    c = conn.cursor()
    success = False
    try:
        columns = ', '.join(row_dict.keys())
        placeholders = ', '.join(['?'] * len(row_dict))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        c.execute(sql, list(row_dict.values()))
        conn.commit()
        success = True
    except Exception as e:
        st.error(f"Lỗi ghi dữ liệu vào {table_name}: {e}")
        return False

    # 2. Ghi vào Google Sheet (Cloud)
    if success:
        try:
            gc = get_gspread_client()
            sh = gc.open_by_key(SPREADSHEET_ID)
            try:
                wks = sh.worksheet(table_name)
            except:
                wks = sh.add_worksheet(title=table_name, rows=100, cols=20)
            
            # Xử lý header và map dữ liệu
            existing = wks.get_all_values()
            if not existing:
                headers = list(row_dict.keys())
                wks.append_row(headers)
            else:
                headers = existing[0]
            
            row_values = []
            for h in headers:
                val = row_dict.get(h, "")
                if val is None: val = ""
                row_values.append(val)
                
            wks.append_row(row_values)
        except Exception as e:
            # [DEBUG] Thay đổi để hiển thị lỗi chi tiết hơn
            st.error(f"⚠️ LỖI ĐỒNG BỘ GOOGLE SHEET (Đã lưu vào máy nhưng không đẩy lên cloud được)")
            st.exception(e)
            
    return success

def upload_to_drive(file_obj, file_name, mimetype=None):
    """Upload file lên Google Drive"""
    try:
        service = get_drive_service()
        file_metadata = {'name': file_name, 'parents': [DRIVE_FOLDER_ID]}
        
        if not mimetype and hasattr(file_obj, 'type'):
            mimetype = file_obj.type
            
        media = MediaIoBaseUpload(file_obj, mimetype=mimetype or 'application/octet-stream', resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        return file.get('webViewLink')
    except Exception as e:
        st.warning(f"⚠️ Lỗi upload Drive: {e}")
        return None

def sync_all_data_to_gsheet():
    """Đọc tất cả dữ liệu từ SQLite và ghi đè lên Google Sheet."""
    TABLES_TO_SYNC = [
        'users', 'invoices', 'projects', 'project_links', 'company_info', 
        'flight_tickets', 'flight_groups', 'flight_group_links', 
        'service_bookings', 'customers', 'tours', 'tour_items', 'ocr_learning',
        'transaction_history'
    ]

    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        conn = get_connection()

        st.info(f"Bắt đầu đồng bộ {len(TABLES_TO_SYNC)} bảng...")
        status_placeholder = st.empty()
        progress_bar = st.progress(0)
        
        for i, table_name in enumerate(TABLES_TO_SYNC):
            status_placeholder.info(f"Đang xử lý bảng: **{table_name}**...")
            
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            except Exception:
                st.warning(f"Bảng '{table_name}' không có trong DB, bỏ qua.")
                progress_bar.progress((i + 1) / len(TABLES_TO_SYNC))
                continue

            try:
                wks = sh.worksheet(table_name)
                wks.clear()
            except gspread.WorksheetNotFound:
                wks = sh.add_worksheet(title=table_name, rows=1, cols=20)

            if not df.empty:
                df = df.astype(str).replace({'nan': '', 'NaT': ''})
                # [FIX] Truncate cells that are too long for Google Sheets API to prevent 400 error
                df = df.map(lambda x: x[:49999] if isinstance(x, str) and len(x) >= 50000 else x)
                data_to_upload = [df.columns.tolist()] + df.values.tolist()
                wks.update(data_to_upload, 'A1')
                st.toast(f"✅ Đồng bộ '{table_name}' ({len(df)} dòng) OK.")
            else:
                st.toast(f"ℹ️ Bảng '{table_name}' rỗng, đã dọn dẹp trên cloud.")

            progress_bar.progress((i + 1) / len(TABLES_TO_SYNC))

        status_placeholder.empty()
        st.success("🎉 Đồng bộ toàn bộ dữ liệu hoàn tất!")
    except Exception as e:
        st.error("❌ Lỗi nghiêm trọng khi đồng bộ:")
        st.exception(e)
        st.info("💡 Gợi ý: Hãy chắc chắn rằng email của tài khoản dịch vụ (`client_email` trong file .json) đã được cấp quyền 'Editor' (Người chỉnh sửa) cho file Google Sheet này.")

# --- QUẢN LÝ SESSION STATE ---
if "logged_in" not in st.session_state: st.session_state.logged_in = False
if "user_info" not in st.session_state: st.session_state.user_info = None
if "db_initialized" not in st.session_state: st.session_state.db_initialized = False

# Biến lưu trữ
if "ready_pdf_bytes" not in st.session_state: st.session_state.ready_pdf_bytes = None
if "ready_file_name" not in st.session_state: st.session_state.ready_file_name = None
if "uploader_key" not in st.session_state: st.session_state.uploader_key = 0
if "pdf_data" not in st.session_state: st.session_state.pdf_data = None
if "edit_lock" not in st.session_state: st.session_state.edit_lock = True
if "local_edit_count" not in st.session_state: st.session_state.local_edit_count = 0
if "current_doc_type" not in st.session_state: st.session_state.current_doc_type = "Hóa đơn"
if "invoice_view_page" not in st.session_state: st.session_state.invoice_view_page = 0

# Biến riêng cho Edit Mode
if "unc_edit_mode" not in st.session_state: st.session_state.unc_edit_mode = False
if "est_edit_mode" not in st.session_state: st.session_state.est_edit_mode = False
if "current_tour_id_est" not in st.session_state: st.session_state.current_tour_id_est = None
if "est_editor_key" not in st.session_state: st.session_state.est_editor_key = 0

# Initialize tab variables to avoid Pylance undefined errors
tab_est = tab_act = tab_rpt = None

# FIX LỖI OUT TÀI KHOẢN
UPLOAD_FOLDER = ".uploaded_invoices"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

DB_FILE = "invoice_app.db"

# ==========================================
# 2. XỬ LÝ DATABASE (SQLite)
# ==========================================
@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def migrate_db_columns():
    conn = get_connection()
    c = conn.cursor()
    # Thêm các cột nếu chưa có cho Hóa đơn/Dự án cũ
    try: c.execute("ALTER TABLE invoices ADD COLUMN request_edit INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE flight_tickets ADD COLUMN airline TEXT")
    except: pass
    try: c.execute("ALTER TABLE projects ADD COLUMN pending_name TEXT")
    except: pass
    try: c.execute("ALTER TABLE projects ADD COLUMN type TEXT DEFAULT 'NORMAL'")
    except: pass
    try: c.execute("ALTER TABLE tour_items ADD COLUMN category TEXT")
    except: pass
    try: c.execute("ALTER TABLE tour_items ADD COLUMN times REAL DEFAULT 1")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN pending_name TEXT")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN request_delete INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN request_edit_act INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN tour_code TEXT")
    except: pass
    try: c.execute("ALTER TABLE invoices ADD COLUMN cost_code TEXT")
    except: pass
    try: c.execute("CREATE TABLE IF NOT EXISTS ocr_learning (keyword TEXT UNIQUE, weight INTEGER DEFAULT 1)")
    except: pass

    # --- Bảng Booking Dịch Vụ (Mới) ---
    try: c.execute('''CREATE TABLE IF NOT EXISTS service_bookings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        name TEXT,
        created_at TEXT,
        status TEXT DEFAULT 'active'
    )''')
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN type TEXT")
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN details TEXT")
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN customer_info TEXT")
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN net_price REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN tax_percent REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN selling_price REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN profit REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE service_bookings ADD COLUMN sale_name TEXT")
    except: pass

    # --- Bảng Khách Hàng (Mới) ---
    try: c.execute('''CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        phone TEXT,
        email TEXT,
        address TEXT,
        notes TEXT,
        created_at TEXT
    )''')
    except: pass
    try: c.execute("ALTER TABLE customers ADD COLUMN sale_name TEXT")
    except: pass

    # --- Cập nhật cột mới cho Tour (Giá chốt, Giá trẻ em, Giá trị hợp đồng) ---
    try: c.execute("ALTER TABLE tours ADD COLUMN final_tour_price REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN child_price REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN contract_value REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN final_qty REAL DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN child_qty REAL DEFAULT 0")
    except: pass

    # --- Cập nhật thông tin khách hàng cho Tour ---
    try: c.execute("ALTER TABLE tours ADD COLUMN customer_name TEXT")
    except: pass
    try: c.execute("ALTER TABLE tours ADD COLUMN customer_phone TEXT")
    except: pass

    # --- Cập nhật mã tour cho dữ liệu cũ ---
    try:
        old_tours = c.execute("SELECT id FROM tours WHERE tour_code IS NULL OR tour_code = ''").fetchall()
        for t in old_tours:
            code = ''.join(random.choices(string.ascii_uppercase, k=5))
            c.execute("UPDATE tours SET tour_code=? WHERE id=?", (code, t['id'])) # type: ignore
    except: pass
    
    # --- Cập nhật dữ liệu cũ để hiện thị dự án ---
    try: 
        c.execute("UPDATE projects SET type='NORMAL' WHERE type IS NULL OR type=''")
    except: pass

    # --- FIX QUAN TRỌNG: ĐẢM BẢO BẢNG TOURS TỒN TẠI KHI CẬP NHẬT ---
    # Phần này giúp tạo bảng ngay cả khi DB đã tồn tại từ trước
    c.execute('''CREATE TABLE IF NOT EXISTS tours (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tour_name TEXT,
        sale_name TEXT,
        start_date TEXT,
        end_date TEXT,
        guest_count INTEGER,
        created_at TEXT,
        est_profit_percent REAL DEFAULT 10.0,
        est_tax_percent REAL DEFAULT 8.0,
        status TEXT DEFAULT 'running'
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS tour_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tour_id INTEGER,
        item_type TEXT, 
        category TEXT,
        description TEXT,
        unit TEXT,
        quantity REAL,
        times REAL DEFAULT 1,
        unit_price REAL,
        total_amount REAL
    )''')
    
    # --- Bảng Công Nợ (Mới) ---
    try: c.execute('''CREATE TABLE IF NOT EXISTS transaction_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ref_code TEXT,
        type TEXT,
        amount REAL,
        payment_method TEXT,
        note TEXT,
        created_at TEXT
    )''')
    except: pass

    conn.commit()

def init_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password TEXT, role TEXT, status TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, date TEXT, invoice_number TEXT, invoice_symbol TEXT, 
        seller_name TEXT, buyer_name TEXT, pre_tax_amount REAL, tax_amount REAL, total_amount REAL, 
        file_name TEXT, status TEXT, edit_count INTEGER, created_at TEXT, memo TEXT, file_path TEXT, request_edit INTEGER DEFAULT 0
    )''')
    # Thêm cột pending_name và type vào bảng projects
    c.execute('''CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        project_name TEXT, 
        created_at TEXT,
        pending_name TEXT,
        type TEXT DEFAULT 'NORMAL'
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS project_links (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, invoice_id INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS company_info (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, address TEXT, phone TEXT, logo_base64 TEXT)''')
    
    # Bảng Vé máy bay
    c.execute('''CREATE TABLE IF NOT EXISTS flight_tickets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_code TEXT,
        flight_date TEXT,
        route TEXT,
        passenger_names TEXT,
        file_path TEXT,
        created_at TEXT,
        airline TEXT
    )''')
    
    # Bảng Đoàn bay (Cũ - Giữ nguyên để tương thích)
    c.execute('''CREATE TABLE IF NOT EXISTS flight_groups (id INTEGER PRIMARY KEY AUTOINCREMENT, group_name TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS flight_group_links (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, ticket_id INTEGER)''')

    # --- BẢNG BOOKING DỊCH VỤ ---
    c.execute('''CREATE TABLE IF NOT EXISTS service_bookings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        name TEXT,
        created_at TEXT,
        status TEXT DEFAULT 'active'
    )''')

    # --- BẢNG QUẢN LÝ TOUR  ---
    c.execute('''CREATE TABLE IF NOT EXISTS tours (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tour_name TEXT,
        sale_name TEXT,
        start_date TEXT,
        end_date TEXT,
        guest_count INTEGER,
        created_at TEXT,
        est_profit_percent REAL DEFAULT 10.0,
        est_tax_percent REAL DEFAULT 8.0,
        status TEXT DEFAULT 'running'
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS tour_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tour_id INTEGER,
        item_type TEXT, 
        category TEXT,
        description TEXT,
        unit TEXT,
        quantity REAL,
        times REAL DEFAULT 1,
        unit_price REAL,
        total_amount REAL
    )''')
    # item_type: 'EST' (Dự toán), 'ACT' (Quyết toán)

    # --- Bảng Công Nợ (Mới) ---
    c.execute('''CREATE TABLE IF NOT EXISTS transaction_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ref_code TEXT,
        type TEXT,
        amount REAL,
        payment_method TEXT,
        note TEXT,
        created_at TEXT
    )''')

    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        admin_pw = hashlib.sha256("admin123".encode()).hexdigest()
        c.execute("INSERT INTO users (username, password, role, status) VALUES (?, ?, ?, ?)", ('admin', admin_pw, 'admin', 'approved'))
    
    c.execute("SELECT * FROM company_info WHERE id = 1")
    if not c.fetchone():
        c.execute("INSERT INTO company_info (name, address, phone, logo_base64) VALUES (?, ?, ?, ?)", ('Tên Công Ty Của Bạn', 'Địa chỉ...', '090...', ''))

    conn.commit()

if not st.session_state.db_initialized:
    init_db()
    st.session_state.db_initialized = True

# Luôn chạy migration để đảm bảo cột mới được thêm vào (Fix lỗi Admin không nhận yêu cầu)
migrate_db_columns()

# --- CÁC HÀM HỖ TRỢ ---
@overload
def run_query(query: str, params: Any = ..., fetch_one: Literal[False] = ..., commit: Literal[False] = ...) -> List[sqlite3.Row]: ...

@overload
def run_query(query: str, params: Any, fetch_one: Literal[True], commit: Literal[False] = ...) -> Optional[sqlite3.Row]: ...

@overload
def run_query(query: str, *, fetch_one: Literal[True], commit: Literal[False] = ...) -> Optional[sqlite3.Row]: ...

@overload
def run_query(query: str, params: Any = ..., fetch_one: Any = ..., *, commit: Literal[True]) -> bool: ...

def run_query(query, params=(), fetch_one=False, commit=False):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(query, params)
        if commit:
            conn.commit()
            return True
        if fetch_one:
            return c.fetchone()
        return c.fetchall()
    except Exception as e:
        print(f"Lỗi truy vấn DB: {e}")
        if commit: return False
        if fetch_one: return None
        return []

def run_query_many(query, data):
    """Thực thi nhiều câu lệnh (thường là INSERT) cùng lúc."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.executemany(query, data)
        conn.commit()
        return True
    except Exception as e:
        print(f"Lỗi truy vấn DB (many): {e}")
        return False

def save_customer_check(name, phone, sale_name=None):
    """Lưu khách hàng mới nếu chưa tồn tại"""
    if not name: return
    try:
        exist = run_query("SELECT id FROM customers WHERE name=?", (name,), fetch_one=True)
        if not exist:
            data = {'name': name, 'phone': phone, 'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            if sale_name:
                data['sale_name'] = sale_name
            add_row_to_table('customers', data)
    except: pass

def hash_pass(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def save_file_local(file_bytes, original_name):
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_name = re.sub(r'[\\/*?:"<>|]', "", original_name)
        if not clean_name.lower().endswith('.pdf'):
            clean_name = os.path.splitext(clean_name)[0] + ".pdf"
            
        final_name = f"{ts}_{clean_name}"
        file_path = os.path.join(UPLOAD_FOLDER, final_name)
        
        with open(file_path, "wb") as f:
            f.write(file_bytes)
                
        return file_path, final_name
    except: return None, None

def format_vnd(amount):
    if amount is None: return "0"
    try: return "{:,.0f}".format(float(amount)).replace(",", ".")
    except: return "0"

@st.cache_data
def get_company_data():
    row = run_query("SELECT * FROM company_info WHERE id = 1", fetch_one=True)
    if isinstance(row, sqlite3.Row):
        return {'name': row['name'], 'address': row['address'], 'phone': row['phone'], 'logo_b64_str': row['logo_base64']}
    return {'name': 'Company', 'address': '...', 'phone': '...', 'logo_b64_str': ''}

def update_company_info(name, address, phone, logo_bytes=None):
    b64_str = base64.b64encode(logo_bytes).decode('utf-8') if logo_bytes else ""
    if not logo_bytes:
        old = run_query("SELECT logo_base64 FROM company_info WHERE id = 1", fetch_one=True)
        if isinstance(old, sqlite3.Row): b64_str = old['logo_base64'] # type: ignore
    run_query("UPDATE company_info SET name=?, address=?, phone=?, logo_base64=? WHERE id=1", (name, address, phone, b64_str), commit=True)
    get_company_data.clear()# type: ignore

def get_tour_financials(tour_id, tour_info):
    """
    Tính toán doanh thu và chi phí cho một tour.
    """
    # Lấy tổng chi phí quyết toán (ACT) từ bảng kê
    act_items = run_query("SELECT SUM(total_amount) as total FROM tour_items WHERE tour_id=? AND item_type='ACT'", (tour_id,), fetch_one=True)
    act_cost_items = act_items['total'] if act_items and act_items['total'] else 0

    # Lấy tổng chi phí từ hóa đơn đầu vào liên kết với tour (không tính UNC)
    inv_items = run_query("SELECT SUM(total_amount) as total FROM invoices WHERE cost_code=? AND status='active' AND type='IN' AND invoice_number NOT LIKE '%UNC%'", (tour_info['tour_code'],), fetch_one=True)
    inv_cost = inv_items['total'] if inv_items and inv_items['total'] else 0

    cost = (act_cost_items or 0) + (inv_cost or 0)

    # Lấy tổng chi phí dự toán (EST) để tính doanh thu nếu cần
    est_items = run_query("SELECT SUM(total_amount) as total FROM tour_items WHERE tour_id=? AND item_type='EST'", (tour_id,), fetch_one=True)
    est_cost = est_items['total'] if est_items and est_items['total'] else 0

    # Tính doanh thu dựa trên giá chốt
    t_dict = dict(tour_info)
    final_price = float(t_dict.get('final_tour_price', 0) or 0)
    child_price = float(t_dict.get('child_price', 0) or 0)
    final_qty = float(t_dict.get('final_qty', 0) or 0)
    child_qty = float(t_dict.get('child_qty', 0) or 0)
    if final_qty == 0: final_qty = float(t_dict.get('guest_count', 1))
    
    revenue = (final_price * final_qty) + (child_price * child_qty)

    # Nếu chi phí quyết toán chưa có, dùng tạm chi phí dự toán
    if cost == 0 and est_cost > 0:
        cost = est_cost

    return revenue, cost
# ==========================================
# 3. CSS & GIAO DIỆN HIỆN ĐẠI
# ==========================================
comp = get_company_data()
st.markdown("""<style>
/* --- BASE & ANIMATION --- */
@keyframes fadeIn { 0% { opacity: 0; transform: translateY(10px); } 100% { opacity: 1; transform: translateY(0); } }
.stApp {
    background-color: #f8f9fa;
    font-family: 'Inter', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    animation: fadeIn 0.5s ease-in-out;
}

/* --- TYPOGRAPHY & LABELS --- */
h1, h2, h3, h4, h5, h6 { color: #2c3e50; }
div[data-testid="stMarkdownContainer"] p { font-weight: 400; white-space: normal; word-break: break-word; }
.company-info-text p, .report-card p { white-space: normal !important; }

/* --- MODERN INPUTS --- */
.stTextInput input, .stNumberInput input, .stSelectbox div[data-baseweb="select"], .stTextArea textarea, .stDateInput input {
    border-radius: 10px !important;
    border: 1px solid #e0e0e0 !important;
    padding: 10px 12px !important;
    background-color: #ffffff !important;
    transition: all 0.3s;
    font-size: 0.95rem;
}
.stTextInput input:focus, .stNumberInput input:focus, .stTextArea textarea:focus, .stDateInput input:focus {
    border-color: #56ab2f !important;
    box-shadow: 0 4px 12px rgba(86, 171, 47, 0.15) !important;
}

/* --- BUTTONS --- */
.stButton button {
    border-radius: 12px !important;
    font-weight: 600;
    font-size: 1rem;
    padding: 0.6rem 1.2rem !important;
    border: none !important;
    box-shadow: 0 4px 6px rgba(0,0,0,0.05);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    white-space: normal !important;
    height: auto !important;
    min-height: 2.5rem;
}
.stButton button:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 15px rgba(0,0,0,0.1);
}
.stButton button[kind="primary"] {
    background: linear-gradient(90deg, #56ab2f 0%, #a8e063 100%);
    color: white;
}
.stButton button[kind="secondary"] {
    background-color: #f1f3f5;
    color: #333;
}

/* --- COMPANY HEADER --- */
.company-header-container {
    display: flex; align-items: center; justify-content: center; gap: 30px;
    padding: 25px 40px; background: rgba(255, 255, 255, 0.8);
    backdrop-filter: blur(10px); border-radius: 20px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.05); margin-bottom: 30px;
    border: 1px solid rgba(255,255,255,0.3); flex-wrap: nowrap !important;
}
.company-logo-img { height: 70px; width: auto; object-fit: contain; flex-shrink: 0; }
.company-info-text { text-align: left; flex: 1; display: flex; flex-direction: column; justify-content: center; white-space: normal; }
.company-info-text h1 { margin: 0; font-size: 1.8rem; color: #2e7d32; font-weight: 800; line-height: 1.2; }
.company-info-text p { margin: 5px 0 0 0; color: #555; font-size: 0.9rem; font-weight: 500; display: flex; align-items: center; gap: 10px; }

/* --- CARD STYLES --- */
.report-card, .login-container {
    background-color: white; border: none; border-radius: 20px;
    padding: 25px; margin-bottom: 25px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.04);
    transition: all 0.3s ease;
}
.report-card:hover { transform: translateY(-5px); box-shadow: 0 20px 40px rgba(0,0,0,0.08); }

/* --- MONEY BOX --- */
.money-box {
    background: linear-gradient(135deg, #00b09b, #96c93d) !important;
    color: #ffffff !important; padding: 25px; border-radius: 20px;
    box-shadow: 0 15px 30px -5px rgba(0, 176, 155, 0.3);
    font-size: clamp(1.2rem, 3vw, 2.5rem); font-weight: 800;
    text-align: center; margin: 1.5rem 0; width: 100%;
    text-shadow: 0 2px 4px rgba(0,0,0,0.1); letter-spacing: 1px;
    white-space: normal; word-wrap: break-word;
    transition: transform 0.3s ease;
}
.money-box:hover { transform: scale(1.02); }

/* --- MODERN TABS --- */
div[data-baseweb="tab-list"] { border-bottom: 2px solid #e0e0e0; }
button[data-baseweb="tab"] {
    background-color: transparent !important; border-bottom: 2px solid transparent !important;
    padding-bottom: 10px !important; margin-bottom: -2px !important; transition: all 0.3s !important;
}
button[data-baseweb="tab"]:hover { background-color: #f1f3f5 !important; }
button[aria-selected="true"] {
    border-bottom-color: #56ab2f !important; font-weight: 600; color: #56ab2f !important;
}

/* --- ENHANCED EXPANDER --- */
div[data-testid="stExpander"] {
    border: 1px solid #e0e0e0 !important; border-radius: 15px !important;
    overflow: hidden; box-shadow: none !important; background-color: #fff;
}
div[data-testid="stExpander"] > details > summary {
    font-weight: 600; font-size: 1.05rem; background-color: #fafafa;
    padding: 0.75rem 1rem !important;
}
div[data-testid="stExpander"] > details > summary:hover { background-color: #f1f3f5; }

/* --- DATA EDITOR --- */
div[data-testid="stDataEditor"] {
    border-radius: 15px; overflow: hidden;
    border: 1px solid #f0f0f0; box-shadow: 0 4px 12px rgba(0,0,0,0.03);
}

/* --- FINANCE SUMMARY CARDS --- */
.finance-summary-card {
    background-color: #ffffff; border: 1px solid #e9ecef; border-radius: 15px;
    padding: 20px; margin-top: 15px;
}
.finance-summary-card .row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 8px 0; border-bottom: 1px solid #f1f3f5;
}
.finance-summary-card .row:last-child { border-bottom: none; }
.finance-summary-card .row span { color: #495057; }
.finance-summary-card .row b { color: #212529; }
.finance-summary-card .total-row {
    font-size: 1.2em; font-weight: bold; color: #2e7d32; padding-top: 15px;
}
.finance-summary-card .pax-price {
    text-align: right; font-size: 0.9em; color: #6c757d; margin-top: 5px;
}
.profit-summary-card {
    background-color: #e3f2fd; padding: 20px; border-radius: 15px;
    text-align: center; border: 1px solid #90caf9; margin-top: 10px;
}
.profit-summary-card h3 {
    margin: 0; color: #1565c0; font-size: 1.1rem; font-weight: 600;
}
.profit-summary-card .formula {
    font-size: 1.8em; font-weight: bold; color: #1e88e5; margin-top: 10px;
}
.profit-summary-card .formula .result { color: #d32f2f; }

/* --- RESPONSIVE --- */
@media only screen and (max-width: 600px) {
    .company-header-container { flex-direction: column; text-align: center; gap: 10px; flex-wrap: wrap !important; }
    .company-info-text { text-align: center; }
    .company-info-text p { justify-content: center; }
}
</style>""", unsafe_allow_html=True)

def convert_image_to_pdf(image_file):
    try:
        img = Image.open(image_file)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        img_width, img_height = img.size
        pdf_buffer = io.BytesIO()
        c = canvas.Canvas(pdf_buffer, pagesize=(img_width, img_height))
        temp_img_path = f"temp_img_{int(time.time())}.jpg"
        img.save(temp_img_path)
        c.drawImage(temp_img_path, 0, 0, img_width, img_height)
        c.save()
        if os.path.exists(temp_img_path): os.remove(temp_img_path)
        pdf_buffer.seek(0)
        return pdf_buffer.getvalue()
    except Exception as e:
        return None

# --- HÀM OCR ---
def perform_ocr(image_input, lang='vie'):
    """
    Thực hiện OCR trên ảnh với các bước tiền xử lý nâng cao sử dụng OpenCV để cải thiện độ chính xác.
    """
    # Check for dependencies and provide clear feedback.
    # This also helps static analysis tools like Pylance understand that `np` and `cv2` are not None below.
    if not HAS_OCR or pytesseract is None:
        st.toast("⚠️ Tesseract OCR chưa được cài đặt.", icon="🚨")
        return ""
    if not HAS_CV or np is None or cv2 is None:
        st.toast("⚠️ OpenCV hoặc Numpy chưa được cài đặt.", icon="🚨")
        return ""
    try:
        # 1. Load ảnh từ input (có thể là file stream hoặc đối tượng PIL)
        if isinstance(image_input, Image.Image):
            img = image_input
        else:
            image_input.seek(0)
            img = Image.open(image_input)

        # 2. Chuyển đổi sang định dạng OpenCV
        # Chuyển sang ảnh xám (grayscale) và numpy array để xử lý
        img_np = np.array(img.convert('L'))

        # 3. Tăng kích thước ảnh (Upscaling)
        # OCR hoạt động tốt hơn với ảnh có DPI cao (khoảng 300). Việc upscale ảnh nhỏ giúp nhận diện ký tự tốt hơn.
        h, w = img_np.shape
        if w < 2000:
            scale = 2000 / w
            new_w, new_h = int(w * scale), int(h * scale)
            # Sử dụng Lanczos interpolation cho kết quả sắc nét khi phóng to
            img_np = cv2.resize(img_np, (new_w, new_h), interpolation=cv2.INTER_LANCZOS4)

        # 4. Giảm nhiễu (Noise Reduction)
        # Sử dụng Median Blur hiệu quả để loại bỏ nhiễu "muối tiêu" (salt-and-pepper noise) mà không làm mờ các cạnh quá nhiều.
        img_np = cv2.medianBlur(img_np, 3)

        # 5. Binarization thông minh (Adaptive Thresholding)
        # Đây là bước quan trọng nhất, thay thế cho việc tăng contrast và dùng ngưỡng cố định.
        # Nó tự động tính toán ngưỡng cho các vùng ảnh nhỏ, rất hiệu quả với ảnh có điều kiện sáng không đồng đều.
        img_processed = cv2.adaptiveThreshold(
            img_np,
            255,  # Giá trị tối đa cho pixel
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,  # Phương pháp tính ngưỡng dựa trên vùng lân cận theo phân phối Gaussian
            cv2.THRESH_BINARY, # Chuyển ảnh thành đen và trắng
            15,  # Kích thước vùng lân cận (block size), nên là số lẻ
            4    # Hằng số C, một giá trị được trừ đi từ giá trị trung bình tính được
        )

        # 6. Cấu hình Tesseract để có kết quả tốt nhất
        # --psm 4: Giả định văn bản là một cột duy nhất với kích thước thay đổi (tốt cho hóa đơn, UNC).
        # --oem 3: Sử dụng engine mặc định (kết hợp Legacy và LSTM), thường cho kết quả ổn định.
        config = '--psm 4 --oem 3'
        text = pytesseract.image_to_string(img_processed, lang='vie+eng', config=config) if pytesseract else ""
        return text
    except Exception as e:
        print(f"OCR Error: {e}")
        return ""

def extract_money_smart(line):
    cleaned = re.sub(r'[^\d.,]', '', line) 
    potential_numbers = []
    raw_digits = re.findall(r'\d+', cleaned)
    for rd in raw_digits:
        if len(rd) > 8 and str(rd).startswith('0'): continue
        if len(rd) >= 4: potential_numbers.append(float(rd))
    matches = re.findall(r'\d[\d.,\s]*\d', line) 
    for m in matches:
        s = m.replace('VND', '').replace('đ', '').replace(' ', '').strip()
        if len(s) > 8 and s.startswith('0'): continue
        try:
            val = 0.0
            if ',' in s and '.' not in s: val = float(s.replace(',', ''))
            elif '.' in s and ',' not in s: val = float(s.replace('.', ''))
            elif ',' in s and '.' in s:
                last_dot = s.rfind('.')
                last_comma = s.rfind(',')
                if last_dot > last_comma: val = float(s.replace(',', '')) 
                else: val = float(s.replace('.', '').replace(',', '.'))
            else: val = float(s)
            if (val > 2030 or val < 1900) and val > 1000:
                potential_numbers.append(val)
        except: pass
    return potential_numbers

def extract_numbers_from_line_basic(line):
    clean_line = line.replace("-", "").replace("VND", "").replace("đ", "").strip()
    raw_integers = re.findall(r'(?<!\d)\d{4,}(?!\d)', clean_line)
    results = []
    for n in raw_integers:
        try:
            val = float(n)
            if not (1990 <= val <= 2030): results.append(val)
        except: pass
    return results

# --- XỬ LÝ HÓA ĐƠN & UNC (LOGIC CŨ) ---
def extract_data_smart(file_obj, is_image, doc_type="Hóa đơn"):
    text_content = ""
    msg = None
    try:
        if is_image:
            if HAS_OCR:
                # Gọi hàm OCR đã sửa đổi
                text_content = perform_ocr(file_obj)
                if not text_content.strip(): msg = "Hic, ảnh mờ quá hoặc không tìm thấy chữ số nào 😭."
            else: msg = "⚠️ Tình yêu ơi, máy chưa cài Tesseract OCR nên không đọc được ảnh nè."
        else:
            # Xử lý PDF (Cả text và scan)
            file_obj.seek(0)
            with pdfplumber.open(file_obj) as pdf:
                for page in pdf.pages: 
                    extracted = page.extract_text()
                    if extracted and len(extracted.strip()) > 10: 
                        text_content += extracted + "\n"
                    else:
                        if HAS_OCR:
                            im = page.to_image(resolution=300).original
                            text_content += perform_ocr(im) + "\n"
            
            if not text_content.strip(): 
                if not HAS_OCR: msg = "⚠️ File PDF này là ảnh scan, cần cài Tesseract OCR để đọc."
                else: msg = "⚠️ File trắng tinh hoặc không đọc được nội dung."

    except Exception as e: return None, f"Lỗi xíu xiu: {str(e)}"
    
    info = {"date": "", "seller": "", "buyer": "", "inv_num": "", "inv_sym": "", "pre_tax": 0.0, "tax": 0.0, "total": 0.0, "content": ""}
    if not text_content: return info, msg

    lines = text_content.split('\n')
    all_found_numbers = set()

    # --- TÌM NGÀY THÁNG ---
    m_date = re.search(r'(?:Ngày|ngày)\s+(\d{1,2})\s+(?:tháng|Tháng|[/.-])\s+(\d{1,2})\s+(?:năm|Năm|[/.-])\s+(\d{4})', text_content)
    if m_date: 
        try: info["date"] = f"{int(m_date.group(1)):02d}/{int(m_date.group(2)):02d}/{m_date.group(3)}"
        except: pass
    else:
        m_date_alt = re.search(r'(\d{2}/\d{2}/\d{4})', text_content)
        if m_date_alt: info["date"] = m_date_alt.group(1)

    # --- LOGIC XỬ LÝ SỐ TIỀN ---
    if doc_type == "Hóa đơn":
        # ... (Giữ nguyên logic Hóa đơn cũ của bạn ở đây nếu cần, hoặc dùng đoạn dưới đây)
        m_no = re.search(r'(?:Số hóa đơn|Số HĐ|Số|No)[:\s\.]*(\d{1,8})\b', text_content, re.IGNORECASE)
        if m_no: info["inv_num"] = m_no.group(1).zfill(7)
        m_sym = re.search(r'(?:Ký hiệu|Mẫu số|Serial)[:\s\.]*([A-Z0-9]{1,2}[A-Z0-9/-]{3,10})', text_content, re.IGNORECASE)
        if m_sym: info["inv_sym"] = m_sym.group(1)
        
        for line in lines:
            line_l = line.lower()
            nums = extract_money_smart(line)
            for n in nums: all_found_numbers.add(n)
            if not nums: continue
            val = max(nums)
            if any(kw in line_l for kw in ["thanh toán", "tổng cộng", "cộng tiền hàng"]): info["total"] = val
            elif any(kw in line_l for kw in ["tiền hàng", "thành tiền", "trước thuế"]): info["pre_tax"] = val
            elif "thuế" in line_l and "suất" not in line_l: info["tax"] = val
        
        if info["total"] == 0 and all_found_numbers: info["total"] = max(all_found_numbers)
        if info["pre_tax"] == 0: info["pre_tax"] = round(info["total"] / 1.08)
        if info["tax"] == 0: info["tax"] = info["total"] - info["pre_tax"]
        
        # Tìm Buyer/Seller cho Hóa đơn
        for line in lines[:35]:
            l_c = line.strip()
            if re.search(r'^(Đơn vị bán|Người bán|Bên A|Nhà cung cấp)', l_c, re.IGNORECASE): 
                parts = l_c.split(':')
                if len(parts) > 1: info["seller"] = parts[-1].strip()
            elif re.search(r'^(Đơn vị mua|Người mua|Khách hàng|Bên B)', l_c, re.IGNORECASE): 
                parts = l_c.split(':')
                if len(parts) > 1: info["buyer"] = parts[-1].strip()

    else: # === UNC (NÂNG CẤP LOGIC) ===
        candidates_total = []
        BLOCK_KEYWORDS = ['số dư', 'balance', 'phí', 'fee', 'charge', 'vat', 'tax', 'điện thoại', 'tel', 'fax', 'mst', 'mã số thuế', 'lệ phí', 'so du', 'le phi']
        CONFIRM_KEYWORDS = ['số tiền', 'amount', 'thanh toán', 'chuyển khoản', 'transaction', 'giá trị', 'total', 'cộng', 'money', 'so tien', 'chuyen khoan', 'gia tri']
        
        # --- LOAD TỪ KHÓA ĐÃ HỌC TỪ DB ---
        learned_kws = run_query("SELECT keyword FROM ocr_learning")
        if learned_kws:
            CONFIRM_KEYWORDS.extend([r['keyword'] for r in learned_kws]) # type: ignore
            
        CURRENCY_KEYWORDS = ['vnd', 'đ', 'vnđ', 'usd']
        prev_line_score_boost = 0
        fallback_numbers = []

        for i, line in enumerate(lines):
            line_l = line.lower()
            
            is_label_line = False
            if any(kw in line_l for kw in CONFIRM_KEYWORDS):
                nums_in_line = extract_money_smart(line)
                if not nums_in_line: 
                    prev_line_score_boost = 15 
                    is_label_line = True
            
            if is_label_line: continue

            nums = extract_money_smart(line)
            if not nums: 
                prev_line_score_boost = 0
                continue
            
            max_val = max(nums)
            if max_val < 1000: 
                prev_line_score_boost = 0
                continue 
            
            is_blocked = any(bad in line_l for bad in BLOCK_KEYWORDS)
            if not is_blocked:
                fallback_numbers.append(max_val)
            
            score = 0
            score += prev_line_score_boost
            prev_line_score_boost = 0 
            
            if any(kw in line_l for kw in CONFIRM_KEYWORDS): score += 10
            if any(kw in line_l for kw in CURRENCY_KEYWORDS): score += 5
            if is_blocked and not any(good in line_l for good in CONFIRM_KEYWORDS):
                score -= 20
            if 'tài khoản' in line_l or 'account' in line_l or 'stk' in line_l: score -= 5

            val_str = "{:,.0f}".format(max_val) # 10,000,000
            val_str_dot = val_str.replace(",", ".") # 10.000.000
            
            if val_str in line or val_str_dot in line:
                score += 3
            elif max_val > 100000000: 
                score -= 3

            if score > -10: candidates_total.append((max_val, score))
        
        if candidates_total:
            candidates_total.sort(key=lambda x: (x[1], x[0]), reverse=True)
            info["total"] = candidates_total[0][0]
        elif fallback_numbers:
            info["total"] = max(fallback_numbers)
            
        info["pre_tax"] = info["total"]
        
        for line in lines:
            if re.search(r'(?:nội dung|diễn giải|lý do|remarks|narrative|description|message)', line, re.IGNORECASE):
                parts = re.split(r'[:\.\-]', line, 1)
                if len(parts) > 1: info["content"] = parts[1].strip()
                else: info["content"] = line.strip()
                break

        for i, line in enumerate(lines):
            line_clean = line.strip()
            if re.search(r'(?:người hưởng|đơn vị thụ hưởng|tài khoản nhận|tên người nhận|bên nhận|beneficiary)', line_clean, re.IGNORECASE):
                parts = line_clean.split(':')
                if len(parts) > 1 and len(parts[-1].strip()) > 3:
                    info["seller"] = parts[-1].strip()
                    break
                elif i + 1 < len(lines):
                    info["seller"] = lines[i+1].strip()
                    break

    info["raw_text"] = text_content
    return info, msg

# ==========================================
# --- MODULE XỬ LÝ AI (GEMINI) & HYBRID ---
# ==========================================

# --- HÀM OCR BẰNG AI (GEMINI) - PHIÊN BẢN FIX LỖI 400 ---
def analyze_invoice_with_gemini(image_file, doc_type="Hóa đơn"):
    """
    Gửi ảnh lên Gemini để trích xuất thông tin JSON.
    Tự động chuẩn hóa ảnh sang JPEG để tránh lỗi 400.
    """
    try:
        # 1. ĐỌC API KEY TỪ FILE JSON
        api_key = None
        try:
            with open('service_account.json', 'r') as f:
                service_info = json.load(f)
                api_key = service_info.get("GEMINI_API_KEY")
        except Exception as e:
            return None, f"Lỗi đọc file service_account.json: {str(e)}"

        if not api_key:
            return None, "⚠️ Không tìm thấy GEMINI_API_KEY trong file service_account.json"
        
        # Cấu hình Gemini
        genai.configure(api_key=api_key) # type: ignore
        
        # 2. CHUẨN HÓA ẢNH (FIX LỖI 400)
        # Mục tiêu: Dù là PDF hay PNG, đều convert về JPEG chuẩn (RGB)
        final_image_bytes = None
        
        try:
            image_file.seek(0)
            file_name = getattr(image_file, 'name', 'unknown').lower()
            
            # TRƯỜNG HỢP 1: FILE PDF -> Chuyển trang đầu thành ảnh
            if file_name.endswith('.pdf'):
                with pdfplumber.open(image_file) as pdf:
                    if len(pdf.pages) > 0:
                        # Lấy trang đầu tiên, độ phân giải cao (300 DPI)
                        page_image = pdf.pages[0].to_image(resolution=300).original
                        
                        # Convert sang RGB (đề phòng) và lưu thành bytes
                        if page_image.mode != 'RGB':
                            page_image = page_image.convert('RGB')
                        
                        img_byte_arr = io.BytesIO()
                        page_image.save(img_byte_arr, format='JPEG', quality=85)
                        final_image_bytes = img_byte_arr.getvalue()
                    else:
                        return None, "File PDF rỗng, không có trang nào."
            
            # TRƯỜNG HỢP 2: FILE ẢNH (PNG, JPG...) -> Convert về JPEG RGB
            else:
                image_pil = Image.open(image_file)
                
                # Xử lý ảnh trong suốt (RGBA) hoặc hệ màu in ấn (CMYK)
                if image_pil.mode in ('RGBA', 'P', 'CMYK'):
                    image_pil = image_pil.convert('RGB')
                
                img_byte_arr = io.BytesIO()
                image_pil.save(img_byte_arr, format='JPEG', quality=85)
                final_image_bytes = img_byte_arr.getvalue()

        except Exception as img_err:
            return None, f"Lỗi xử lý ảnh đầu vào: {str(img_err)}"

        if not final_image_bytes:
            return None, "Không thể tạo dữ liệu ảnh để gửi đi."

        # Đóng gói dữ liệu gửi đi (Luôn là image/jpeg)
        image_part = {"mime_type": "image/jpeg", "data": final_image_bytes}

        # 3. Tạo Prompt
        prompt = f"""
        Bạn là kế toán viên chuyên nghiệp. Hãy trích xuất thông tin từ hình ảnh {doc_type} này thành dữ liệu JSON.
        
        Yêu cầu bắt buộc:
        1. Trả về kết quả CHỈ LÀ MỘT JSON thuần.
        2. Các trường cần lấy:
           - date: ngày chứng từ (DD/MM/YYYY).
           - seller: tên đơn vị bán / người thụ hưởng.
           - buyer: tên đơn vị mua / người trả tiền.
           - inv_num: số hóa đơn / số bút toán.
           - inv_sym: ký hiệu (nếu có).
           - pre_tax: thành tiền trước thuế (số nguyên).
           - tax: tiền thuế (số nguyên).
           - total: tổng thanh toán (số nguyên).
           - content: nội dung diễn giải chính.
        
        Nếu không có thông tin, hãy để 0 hoặc "".
        """

        # 4. TỰ ĐỘNG CHỌN MODEL
        active_model_name = 'models/gemini-1.5-flash' # Mặc định dùng Flash
        
        # Thử lấy model tốt nhất
        try:
            for m in genai.list_models(): # type: ignore
                if 'generateContent' in m.supported_generation_methods:
                    if 'flash' in m.name:
                        active_model_name = m.name
                        break
        except: pass

        # 5. Gọi Model
        model = genai.GenerativeModel(active_model_name) # type: ignore
        response = model.generate_content([prompt, image_part])
        
        # 6. Xử lý kết quả trả về
        if not response.text:
            return None, "AI không trả về kết quả (Response empty)."

        raw_text = response.text.strip()
        if raw_text.startswith("```json"): raw_text = raw_text[7:]
        if raw_text.endswith("```"): raw_text = raw_text[:-3]
            
        data = json.loads(raw_text)
        
        info = {
            "date": data.get("date", ""),
            "seller": data.get("seller", ""),
            "buyer": data.get("buyer", ""),
            "inv_num": data.get("inv_num", ""),
            "inv_sym": data.get("inv_sym", ""),
            "pre_tax": float(data.get("pre_tax", 0)),
            "tax": float(data.get("tax", 0)),
            "total": float(data.get("total", 0)),
            "content": data.get("content", ""),
            "note": f"✨ AI ({active_model_name})" 
        }
        return info, None

    except Exception as e:
        return None, f"Lỗi AI: {str(e)}"

def extract_data_hybrid(file_obj, is_image, doc_type="Hóa đơn"):
    """
    Chế độ Lai ghép: Ưu tiên AI -> Nếu lỗi thì dùng Tesseract
    """
    # CÁCH 1: Thử dùng AI trước
    try:
        file_obj.seek(0) # Reset con trỏ file
        data, error = analyze_invoice_with_gemini(file_obj, doc_type)
        
        if data and not error:
            return data, None
        else:
            print(f"AI thất bại, chuyển sang OCR thường. Lỗi: {error}")
    except Exception as e:
        print(f"Lỗi nghiêm trọng AI: {e}")

    # CÁCH 2: Fallback về Tesseract (OCR thường)
    try:
        file_obj.seek(0) # Reset con trỏ file lần nữa
        st.toast("⚠️ AI đang bận, đang dùng công nghệ cũ...", icon="🔄")
        
        # Gọi hàm cũ của bạn
        data, msg = extract_data_smart(file_obj, is_image, doc_type)
        if data:
            data['note'] = "📷 Xử lý bởi Tesseract (Offline)"
        return data, msg
    except Exception as e:
        return None, f"Lỗi toàn hệ thống: {str(e)}"

# ==========================================
# 4. GIAO DIỆN & LOGIC MODULES
# ==========================================

def render_login_page(comp):
    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_b:
        st.write("")
        if comp['logo_b64_str']:
            st.markdown(f'''
            <div class="company-header-container">
                <img src="data:image/png;base64,{comp["logo_b64_str"]}" class="company-logo-img">
                <div class="company-info-text">
                    <h1>{comp['name']}</h1>
                    <p>📍 {comp['address']}</p>
                    <p>📞 {comp['phone']}</p>
                </div>
            </div>
            ''', unsafe_allow_html=True)
        else:
            st.markdown(f"""<div style="text-align:center; margin-top:20px;"><h1 style="color:#28a745 !important;">{comp['name']}</h1><p>📍 {comp['address']}<br>📞 {comp['phone']}</p></div>""", unsafe_allow_html=True)
        
        tab_login, tab_reg = st.tabs(["🔐 Đăng nhập", "📝 Đăng ký"])
        with tab_login:
            with st.container(border=True):
                with st.form("login"):
                    u = st.text_input("Tài khoản"); p = st.text_input("Mật khẩu", type="password")
                    if st.form_submit_button("ĐĂNG NHẬP", width="stretch"):
                        pw_hash = hash_pass(p)
                        
                        # [CODE MỚI] Đọc từ Google Sheet thay vì SQL
                        df_users = load_table('users') 
                        
                        # Kiểm tra user
                        if not df_users.empty:
                            # Lọc user trùng username và password
                            mask = (df_users['username'] == u) & (df_users['password'] == pw_hash) # type: ignore
                            user_found = df_users.loc[mask]
                            
                            if not user_found.empty and user_found.iloc[0]['status'] == 'approved': # type: ignore
                                st.session_state.logged_in = True
                                st.session_state.user_info = {
                                    "name": user_found.iloc[0]['username'],  # type: ignore
                                    "role": user_found.iloc[0]['role'] # type: ignore
                                }
                                st.rerun()
                            else:
                                st.error("Sai thông tin hoặc tài khoản chưa duyệt!")
                        else:
                            st.error("Không kết nối được danh sách người dùng!")
        with tab_reg:
            with st.container(border=True):
                with st.form("reg"):
                    nu = st.text_input("Tài khoản mới"); np = st.text_input("Mật khẩu", type="password")
                    if st.form_submit_button("ĐĂNG KÝ", width="stretch"):
                        try:
                            add_row_to_table('users', {'username': nu, 'password': hash_pass(np), 'role': 'user', 'status': 'pending'})
                            st.success("Đã gửi yêu cầu! Chờ xíu nha 🥰")
                        except: st.error("Tên này có người dùng rồi nè!")

def render_admin_notifications():
    st.divider()
    st.markdown("### 🔔 Trung Tâm Thông Báo & Phê Duyệt")
    
    # --- LẤY DỮ LIỆU CẦN DUYỆT ---
    pending_projs = run_query("SELECT * FROM projects WHERE pending_name IS NOT NULL AND pending_name != ''")
    pending_tours = run_query("SELECT * FROM tours WHERE pending_name IS NOT NULL AND pending_name != ''")
    del_tours = run_query("SELECT * FROM tours WHERE request_delete=1")
    req_edit_tours = run_query("SELECT * FROM tours WHERE request_edit_act=1")
    pending_users = run_query("SELECT * FROM users WHERE role='user' AND status='pending'")
    req_invoices = run_query("SELECT * FROM invoices WHERE request_edit=1 AND status='active'")
    
    has_requests = False

    # 1. DUYỆT ĐỔI TÊN DỰ ÁN
    if pending_projs:
        has_requests = True
        st.markdown(f"#### 📝 Đổi tên Dự án ({len(pending_projs)})")
        for p in pending_projs:
            with st.container(border=True):
                st.markdown(f"**Dự án:** `{p['project_name']}` ➡ <span style='color:green'><b>`{p['pending_name']}`</b></span>", unsafe_allow_html=True) # type: ignore
                c_app, c_rej = st.columns(2)
                if c_app.button("✔ Duyệt", key=f"app_ren_{p['id']}", type="primary"): # type: ignore
                    run_query("UPDATE projects SET project_name=?, pending_name=NULL WHERE id=?", (p['pending_name'], p['id']), commit=True) # type: ignore
                    st.rerun()
                if c_rej.button("✖ Hủy", key=f"rej_ren_{p['id']}"): # type: ignore
                    run_query("UPDATE projects SET pending_name=NULL WHERE id=?", (p['id'],), commit=True) # type: ignore
                    st.rerun()

    # 2. DUYỆT ĐỔI TÊN TOUR
    if pending_tours:
        has_requests = True
        st.markdown(f"#### 📦 Đổi tên Tour ({len(pending_tours)})")
        for t in pending_tours:
            with st.container(border=True):
                st.markdown(f"**Tour:** `{t['tour_name']}` ➡ <span style='color:green'><b>`{t['pending_name']}`</b></span>", unsafe_allow_html=True) # type: ignore
                c_app, c_rej = st.columns(2)
                if c_app.button("✔ Duyệt", key=f"app_ren_t_{t['id']}", type="primary"): # type: ignore
                    run_query("UPDATE tours SET tour_name=?, pending_name=NULL WHERE id=?", (t['pending_name'], t['id']), commit=True) # type: ignore
                    st.rerun()
                if c_rej.button("✖ Hủy", key=f"rej_ren_t_{t['id']}"): # type: ignore
                    run_query("UPDATE tours SET pending_name=NULL WHERE id=?", (t['id'],), commit=True) # type: ignore
                    st.rerun()

    # 3. DUYỆT XÓA TOUR
    if del_tours:
        has_requests = True
        st.markdown(f"#### <span style='color:red;'>🗑️ Xóa Tour ({len(del_tours)})</span>", unsafe_allow_html=True)
        for t in del_tours:
            with st.container(border=True):
                st.markdown(f"❌ Yêu cầu xóa Tour: **{t['tour_name']}**") # type: ignore
                c_app, c_rej = st.columns(2)
                if c_app.button("✔ Duyệt xóa", key=f"app_del_t_{t['id']}", type="primary"): # type: ignore
                    run_query("UPDATE tours SET request_delete=2 WHERE id=?", (t['id'],), commit=True) # type: ignore
                    st.success("Đã duyệt! Chờ người dùng xác nhận."); time.sleep(1); st.rerun()
                if c_rej.button("✖ Từ chối", key=f"rej_del_t_{t['id']}"): # type: ignore
                    run_query("UPDATE tours SET request_delete=0 WHERE id=?", (t['id'],), commit=True) # type: ignore
                    st.rerun()

    # 4. DUYỆT SỬA QUYẾT TOÁN (MỚI)
    if req_edit_tours:
        has_requests = True
        st.markdown(f"#### 💸 Sửa Quyết toán ({len(req_edit_tours)})")
        for t in req_edit_tours:
            with st.container(border=True):
                st.write(f"Tour: **{t['tour_name']}**") # type: ignore
                c1, c2 = st.columns(2)
                if c1.button("✔ Duyệt", key=f"app_edit_act_{t['id']}"): # type: ignore
                    run_query("UPDATE tours SET request_edit_act=2 WHERE id=?", (t['id'],), commit=True); st.rerun() # type: ignore
                if c2.button("✖ Từ chối", key=f"rej_edit_act_{t['id']}"): # type: ignore
                    run_query("UPDATE tours SET request_edit_act=0 WHERE id=?", (t['id'],), commit=True); st.rerun() # type: ignore

    # 5. DUYỆT USER
    if pending_users:
        has_requests = True
        st.markdown(f"#### 👤 Đăng ký mới ({len(pending_users)})")
        for u in pending_users:
            with st.container(border=True):
                st.write(f"User: **{u['username']}**") # type: ignore
                c1, c2 = st.columns(2)
                if c1.button("✔ Duyệt", key=f"app_user_{u['id']}"): # type: ignore
                    run_query("UPDATE users SET status='approved' WHERE id=?", (u['id'],), commit=True) # type: ignore
                    st.rerun()
                if c2.button("✖ Xóa", key=f"del_user_{u['id']}"): # type: ignore
                    run_query("DELETE FROM users WHERE id=?", (u['id'],), commit=True) # type: ignore
                    st.rerun()

    # 6. DUYỆT SỬA GIÁ HÓA ĐƠN
    if req_invoices:
        has_requests = True
        st.markdown(f"#### 💰 Sửa giá Hóa đơn ({len(req_invoices)})")
        for r in req_invoices:
            with st.container(border=True):
                st.info(f"HĐ: {r['invoice_number']} | Tiền: {format_vnd(r['total_amount'])}") # type: ignore
                c1, c2 = st.columns(2)
                if c1.button("✔ Duyệt", key=f"app_inv_{r['id']}"): # type: ignore
                    run_query("UPDATE invoices SET edit_count=0, request_edit=0 WHERE id=?", (r['id'],), commit=True) # type: ignore
                    st.success("Đã duyệt!"); time.sleep(0.5); st.rerun()
                if c2.button("✖ Từ chối", key=f"rej_inv_{r['id']}"): # type: ignore
                    run_query("UPDATE invoices SET request_edit=0 WHERE id=?", (r['id'],), commit=True) # type: ignore
                    st.rerun()

    if not has_requests:
        st.success("✅ Hiện không có yêu cầu nào cần duyệt.")

def render_admin_panel(comp):
    with st.expander("⚙️ Admin Panel", expanded=False):
        st.caption("Cập nhật thông tin Công ty")
        with st.form("comp_update"):
            cn = st.text_input("Tên", value=comp['name'])
            ca = st.text_input("Địa chỉ", value=comp['address'])
            cp = st.text_input("SĐT", value=comp['phone'])
            ul = st.file_uploader("Logo", type=['png','jpg'])
            if st.form_submit_button("Lưu"):
                update_company_info(cn, ca, cp, ul.read() if ul else None)
                st.success("Xong!"); time.sleep(0.5); st.rerun()
        
        # Chỉ admin chính mới thấy mục xóa
        if (st.session_state.user_info or {}).get('role') == 'admin':
            st.divider()
            st.markdown("##### 🗑️ Quản lý dữ liệu")
            
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Xóa Hóa Đơn", use_container_width=True, help="Xóa TOÀN BỘ dữ liệu Hóa đơn & UNC"):
                    run_query("DELETE FROM invoices", commit=True)
                    run_query("DELETE FROM sqlite_sequence WHERE name='invoices'", commit=True)
                    if os.path.exists(UPLOAD_FOLDER):
                        for f in os.listdir(UPLOAD_FOLDER):
                            if "UNC" not in f and "converted" not in f: 
                                    try: os.remove(os.path.join(UPLOAD_FOLDER, f))
                                    except: pass
                    st.toast("Đã xóa sạch Hóa Đơn!"); time.sleep(1); st.rerun()
                
                if st.button("Xóa Tour", use_container_width=True, help="Xóa TOÀN BỘ dữ liệu Tour (Dự toán và Quyết toán)"):
                    run_query("DELETE FROM tours", commit=True)
                    run_query("DELETE FROM tour_items", commit=True)
                    run_query("DELETE FROM sqlite_sequence WHERE name='tours'", commit=True)
                    run_query("DELETE FROM sqlite_sequence WHERE name='tour_items'", commit=True)
                    st.toast("Đã xóa sạch dữ liệu Tour!"); time.sleep(1); st.rerun()
            
            with c2:
                if st.button("Xóa Booking", use_container_width=True, help="Xóa TOÀN BỘ dữ liệu Booking dịch vụ"):
                    run_query("DELETE FROM service_bookings", commit=True)
                    run_query("DELETE FROM sqlite_sequence WHERE name='service_bookings'", commit=True)
                    st.toast("Đã xóa sạch Booking!"); time.sleep(1); st.rerun()
                
                if st.button("Xóa Khách Hàng", use_container_width=True, help="Xóa TOÀN BỘ dữ liệu Khách hàng"):
                    run_query("DELETE FROM customers", commit=True); run_query("DELETE FROM sqlite_sequence WHERE name='customers'", commit=True)
                    st.toast("Đã xóa sạch Khách hàng!"); time.sleep(1); st.rerun()

            with st.popover("💥 XÓA TOÀN BỘ DỮ LIỆU 💥", use_container_width=True):
                st.error("CẢNH BÁO CỰC KỲ NGUY HIỂM!")
                st.warning("Hành động này sẽ **XÓA SẠCH TOÀN BỘ** dữ liệu kinh doanh (Hóa đơn, Tour, Booking, Khách hàng...). Dữ liệu người dùng và thông tin công ty sẽ được giữ lại. Hành động này không thể hoàn tác.")
                st.warning("Chỉ thực hiện khi bạn muốn bắt đầu lại từ đầu. Bạn có chắc chắn không?")
                if st.button("CÓ, TÔI HIỂU RỦI RO VÀ MUỐN XÓA TẤT CẢ", type="primary"):
                    TABLES_TO_DELETE = [
                        'invoices', 'projects', 'project_links', 'service_bookings', 
                        'customers', 'tours', 'tour_items', 'ocr_learning',
                        'transaction_history',
                        'flight_tickets', 'flight_groups', 'flight_group_links'
                    ]
                    with st.spinner("Đang dọn dẹp hệ thống..."):
                        for table in TABLES_TO_DELETE:
                            run_query(f"DELETE FROM {table}", commit=True)
                            run_query(f"DELETE FROM sqlite_sequence WHERE name='{table}'", commit=True)
                        if os.path.exists(UPLOAD_FOLDER):
                            for f in os.listdir(UPLOAD_FOLDER):
                                try: os.remove(os.path.join(UPLOAD_FOLDER, f))
                                except: pass
                    st.success("Đã xóa toàn bộ dữ liệu kinh doanh và các file đã upload!")
                    time.sleep(2); st.rerun()

        with st.popover("🔄 Đồng bộ lên Google Sheet", use_container_width=True):
            st.warning("⚠️ Hành động này sẽ **ghi đè toàn bộ** dữ liệu trên Google Sheet bằng dữ liệu hiện tại trên máy của bạn. Bạn có chắc chắn không?")
            if st.button("Có, tôi muốn đồng bộ ngay", type="primary"):
                sync_all_data_to_gsheet()

def render_sidebar(comp):
    with st.sidebar:
        if comp['logo_b64_str']: st.markdown(f'<div style="text-align:center; margin-bottom:20px;"><img src="data:image/png;base64,{comp["logo_b64_str"]}" width="120" style="border-radius:10px;"></div>', unsafe_allow_html=True)
        
        user_info = st.session_state.get("user_info")
        if user_info and isinstance(user_info, dict):
            st.success(f"Xin chào **{user_info.get('name', 'User')}** 👋")
        else:
            st.session_state.logged_in = False
            st.rerun()
        
        st.markdown("### 🗂️ Phân Hệ Quản Lý")
        module = st.selectbox("Chọn chức năng:", ["🔖 Quản Lý Booking", "💰 Kiểm Soát Chi Phí", "💳 Quản Lý Công Nợ", "📦 Quản Lý Tour ", "🤝 Quản Lý Khách Hàng", "👥 Quản Lý Nhân Sự", "🔍 Tra cứu thông tin"], label_visibility="collapsed")
        
        menu = None
        if module == "💰 Kiểm Soát Chi Phí":
            menu = st.radio("Menu", ["1. Nhập Hóa Đơn", "2. Báo Cáo Tổng Hợp"])
        
        if st.session_state.user_info and st.session_state.user_info.get('role') in ['admin', 'admin_f1']:
            render_admin_notifications()

        st.divider()

        if st.session_state.user_info and st.session_state.user_info.get('role') in ['admin', 'admin_f1']:
            render_admin_panel(comp)

        if st.button("Đăng xuất", use_container_width=True):
            st.session_state.logged_in = False
            st.rerun()
        with st.popover("🔐 Đổi mật khẩu", use_container_width=True):
            st.markdown("##### Cập nhật mật khẩu")
            with st.form("change_pass"):
                op = st.text_input("Mật khẩu hiện tại", type="password")
                new_p = st.text_input("Mật khẩu mới", type="password")
                cp = st.text_input("Xác nhận mật khẩu mới", type="password")
                if st.form_submit_button("Lưu thay đổi"):
                    c_user = (st.session_state.user_info or {}).get('name', '')
                    db_u = run_query("SELECT * FROM users WHERE username=?", (c_user,), fetch_one=True)
                    if isinstance(db_u, sqlite3.Row) and db_u['password'] == hash_pass(op): # type: ignore
                        if new_p and new_p == cp:
                            run_query("UPDATE users SET password=? WHERE username=?", (hash_pass(new_p), c_user), commit=True)
                            st.success("Đổi mật khẩu thành công! Đăng nhập lại nhé.")
                            time.sleep(1)
                            st.session_state.logged_in = False
                            st.rerun()
                        else:
                            st.error("Mật khẩu mới không khớp!")
                    else:
                        st.error("Mật khẩu cũ sai rồi!")

        # --- KIỂM TRA KẾT NỐI GOOGLE (DEBUG) ---
        st.divider() # type: ignore
        with st.expander("🔌 Kiểm tra kết nối Google"):
            if st.button("Test Kết Nối Ngay", use_container_width=True):
                try:
                    with st.spinner("Đang kết nối Google API..."):
                        gc = get_gspread_client()
                        sh = gc.open_by_key(SPREADSHEET_ID)
                        st.success(f"✅ Sheet OK: {sh.title}")
                        drive = get_drive_service()
                        st.success(f"✅ Drive OK (ID: ...{DRIVE_FOLDER_ID[-5:]})")
                except Exception as e:
                    st.error(f"❌ Lỗi: {str(e)}")
                    st.info("💡 Gợi ý: Kiểm tra file service_account.json hoặc quyền chia sẻ của Sheet/Folder.")
    return module, menu

# --- HÀM HIỂN THỊ SO SÁNH CHI PHÍ (UNC vs HÓA ĐƠN) ---
def render_cost_comparison(code):
    # Lấy tất cả hóa đơn/UNC theo mã
    docs = run_query("SELECT * FROM invoices WHERE cost_code=? AND status='active'", (code,))
    if not docs:
        st.info("Chưa có chứng từ nào liên kết.")
        return 0

    df = pd.DataFrame([dict(r) for r in docs])
    
    # Lọc chi phí đầu vào (IN)
    df_in = df.loc[df['type'] == 'IN'].copy() # type: ignore
    if df_in.empty:
        st.info("Chưa có chi phí đầu vào.")
        return 0

    # Tách Hóa đơn và UNC (Dựa vào số hóa đơn có chứa 'UNC' hay không)
    df_in['Is_UNC'] = df_in['invoice_number'].astype(str).str.contains("UNC", case=False, na=False) # type: ignore
    
    df_bills = df_in.loc[~df_in['Is_UNC']]
    df_uncs = df_in.loc[df_in['Is_UNC']]
    
    total_bills = df_bills['total_amount'].sum()
    total_uncs = df_uncs['total_amount'].sum()
    
    # Hiển thị so sánh
    c1, c2, c3 = st.columns(3)
    c1.metric("Tổng Hóa Đơn (Chi phí)", format_vnd(total_bills), help="Tổng giá trị các hóa đơn đầu vào (Không tính UNC)")
    c2.metric("Tổng UNC (Đã chi)", format_vnd(total_uncs), help="Tổng số tiền đã chuyển khoản (UNC)")
    
    diff = total_uncs - total_bills
    if diff == 0:
        c3.success("✅ Đã khớp")
    elif diff > 0:
        c3.warning(f"⚠️ UNC dư: {format_vnd(diff)}")
    else:
        c3.error(f"⚠️ Thiếu UNC: {format_vnd(abs(diff))}")
        
    # Bảng chi tiết
    t1, t2 = st.tabs(["📄 Danh sách Hóa Đơn", "💸 Danh sách UNC"])
    with t1:
        st.dataframe(df_bills[['date', 'invoice_number', 'seller_name', 'total_amount', 'memo']], 
                     column_config={"total_amount": st.column_config.NumberColumn("Số tiền", format="%d")}, use_container_width=True, hide_index=True)
    with t2:
        st.dataframe(df_uncs[['date', 'invoice_number', 'seller_name', 'total_amount', 'memo']], 
                     column_config={"total_amount": st.column_config.NumberColumn("Số tiền", format="%d")}, use_container_width=True, hide_index=True)
        
    return total_bills

def render_cost_control(menu):
    if menu == "1. Nhập Hóa Đơn":
        # 1. Logic Nhập UNC mặc định là Đầu vào (Nhưng Type IN)
        doc_type = st.radio("📂 Loại chứng từ", ["Ủy nhiệm chi ", "Hóa đơn"], horizontal=True, index=1 if st.session_state.current_doc_type == "Hóa đơn" else 0)
        
        if doc_type != st.session_state.current_doc_type:
            st.session_state.current_doc_type = doc_type
            st.session_state.pdf_data = None
            st.session_state.ready_pdf_bytes = None
            st.session_state.ready_file_name = None
            st.session_state.uploader_key += 1
            st.rerun()

        uploaded_file = st.file_uploader(f"Upload {doc_type} (PDF/Ảnh)", type=["pdf", "png", "jpg", "jpeg"], key=f"up_{st.session_state.uploader_key}")
        
        if uploaded_file and st.session_state.ready_file_name != uploaded_file.name:
            st.session_state.ready_pdf_bytes = None
            st.session_state.ready_file_name = uploaded_file.name
            st.session_state.pdf_data = None
            st.session_state.invoice_view_page = 0
        
        is_ready_to_analyze = False
        is_pdf_origin = False
        
        if uploaded_file:
            file_type = uploaded_file.type
            is_pdf_origin = "pdf" in file_type
            is_ready_to_analyze = True

            c_view, c_action = st.columns([1, 1])
            with c_view:
                if is_pdf_origin:
                    st.info("📄 File PDF Gốc")
                    pdf_img = None
                    total_pages = 0
                    try:
                        uploaded_file.seek(0)
                        with pdfplumber.open(uploaded_file) as pdf:
                            total_pages = len(pdf.pages)
                            if st.session_state.invoice_view_page >= total_pages: st.session_state.invoice_view_page = 0
                            pdf_img = pdf.pages[st.session_state.invoice_view_page].to_image(resolution=200).original
                    except: pass
                    
                    if total_pages > 0:
                        if total_pages > 1:
                            c_p, c_n = st.columns(2)
                            if c_p.button("⬅ Trước", key="btn_inv_prev", use_container_width=True): st.session_state.invoice_view_page = max(0, st.session_state.invoice_view_page - 1); st.rerun()
                            if c_n.button("Sau ➡", key="btn_inv_next", use_container_width=True): st.session_state.invoice_view_page = min(total_pages - 1, st.session_state.invoice_view_page + 1); st.rerun()
                        if pdf_img:
                            st.image(pdf_img, caption=f"Trang {st.session_state.invoice_view_page+1}/{total_pages}", width="stretch")
                else:
                    st.info("🖼️ File Ảnh")
                    st.image(uploaded_file, caption="Ảnh gốc", width="stretch")
                    
            with c_action:
                if not is_pdf_origin and st.session_state.ready_pdf_bytes is None:
                    st.info("👉 Bạn đang dùng File Ảnh. Hệ thống sẽ dùng OCR để quét.")
                    if st.button("🔄 CHUYỂN ĐỔI SANG PDF (ĐỂ LƯU TRỮ)", type="secondary", width="stretch"):
                        with st.spinner("Đang chuyển đổi..."):
                            uploaded_file.seek(0)
                            converted_bytes = convert_image_to_pdf(uploaded_file)
                            if converted_bytes:
                                st.session_state.ready_pdf_bytes = converted_bytes
                                st.success("Đã convert xong!")
                                time.sleep(0.5)
                                st.rerun()

                if is_ready_to_analyze:
                    # Thêm lựa chọn chế độ quét
                    scan_mode = st.radio(
                        "Công nghệ quét:", 
                        ["🚀 Tự động (Hybrid: AI -> Tesseract)", "⚡ Chỉ dùng AI (Gemini)", "📷 Chỉ dùng Tesseract"], 
                        horizontal=True
                    )

                    if st.button(f"🔍 QUÉT THÔNG TIN ({doc_type})", type="primary", width="stretch"):
                        # Logic xác định loại file cho hàm cũ
                        file_to_scan = uploaded_file
                        is_img_input = "pdf" not in uploaded_file.type
                        
                        data = None
                        msg = None

                        with st.spinner("Đang phân tích dữ liệu..."):
                            if "Tự động" in scan_mode:
                                # Dùng hàm Hybrid mới
                                data, msg = extract_data_hybrid(file_to_scan, is_img_input, doc_type)
                            
                            elif "Chỉ dùng AI" in scan_mode:
                                # Chỉ gọi Gemini
                                file_to_scan.seek(0)
                                data, msg = analyze_invoice_with_gemini(file_to_scan, doc_type)
                                
                            else: 
                                # Chỉ gọi hàm cũ (Tesseract)
                                file_to_scan.seek(0)
                                data, msg = extract_data_smart(file_to_scan, is_img_input, doc_type)
                                if data: data['note'] = "📷 Xử lý bởi Tesseract"

                        # --- Hiển thị kết quả ---
                        if msg: st.warning(msg)
                        
                        if data:
                            # Thông báo thành công & Nguồn dữ liệu
                            st.success(f"✅ Đã quét xong! ({data.get('note', '')})")
                            
                            # Lưu vào Session State
                            data['file_name'] = uploaded_file.name
                            st.session_state.pdf_data = data
                            st.session_state.edit_lock = True
                            st.session_state.local_edit_count = 0
                            
                            # Nếu là Hóa đơn, kiểm tra lệch tiền
                            if doc_type == "Hóa đơn":
                                diff = abs(data['total'] - (data['pre_tax'] + data['tax']))
                                if diff < 10: st.caption("✅ Kiểm tra: Tổng tiền khớp.")
                                else: st.warning(f"⚠️ Kiểm tra: Lệch {format_vnd(diff)}")
                            
                            time.sleep(0.5)
                            st.rerun()

                if st.session_state.pdf_data:
                    d = st.session_state.pdf_data
                    st.divider()
                    
                    # --- LOGIC MÃ CHI PHÍ (COST CODE) - MOVED OUTSIDE FORM ---
                    # Lấy danh sách Tour đang chạy để chọn
                    user_info_cost = st.session_state.get("user_info", {})
                    user_role_cost = user_info_cost.get('role')
                    user_name_cost = user_info_cost.get('name')
                    tour_query = "SELECT tour_name, tour_code FROM tours WHERE status='running'"
                    tour_params = []
                    if user_role_cost == 'sale' and user_name_cost:
                        tour_query += " AND sale_name=?"
                        tour_params.append(user_name_cost)
                    active_tours = run_query(tour_query, tuple(tour_params))
                    tour_choices = {f"[{t['tour_code']}] {t['tour_name']}": t['tour_code'] for t in active_tours} if active_tours else {} # type: ignore
                    tour_choices = {f"📦 TOUR: [{t['tour_code']}] {t['tour_name']}": t['tour_code'] for t in active_tours} if active_tours else {} # type: ignore
                    
                    # Lấy danh sách các mã Cost Code đã tồn tại (từ UNC hoặc Hóa đơn trước đó) để Hóa đơn chọn lại
                    existing_codes_query = run_query("SELECT DISTINCT cost_code FROM invoices WHERE cost_code IS NOT NULL AND cost_code != ''")
                    existing_codes = [r['cost_code'] for r in existing_codes_query] if existing_codes_query else [] # type: ignore
                    
                    # Lấy danh sách Booking Dịch Vụ (Lọc theo sale nếu cần)
                    bk_query = "SELECT name, code FROM service_bookings WHERE status='active'"
                    bk_params = []
                    if user_role_cost == 'sale' and user_name_cost:
                        bk_query += " AND sale_name=?"
                        bk_params.append(user_name_cost)
                    active_bookings = run_query(bk_query, tuple(bk_params))
                    booking_choices = {f"🔖 BOOKING: [{b['code']}] {b['name']}": b['code'] for b in active_bookings} if active_bookings else {} # type: ignore

                    selected_cost_code = ""
                    new_bk_name = None
                    new_bk_code = None
                    
                    st.markdown("##### 🔖 Phân loại & Liên kết chi phí")
                    with st.container(border=True):
                        if doc_type == "Ủy nhiệm chi ":
                            st.info("🔖 Phân loại chi phí")
                            # Logic mới: Luôn yêu cầu chọn Mã (Tour hoặc Booking)
                            link_type = st.radio("Liên kết với:", ["Tour", "Booking Dịch Vụ"], horizontal=True)
                            
                            if link_type == "Tour":
                                if tour_choices:
                                    sel_t = st.selectbox("Chọn Tour:", list(tour_choices.keys()))
                                    selected_cost_code = tour_choices[sel_t]
                                else:
                                    st.warning("Chưa có Tour nào đang chạy.")
                            else:
                                # Booking Dịch Vụ
                                bk_action = st.radio("Thao tác:", ["Chọn Booking có sẵn", "➕ Tạo Booking mới"], horizontal=True, label_visibility="collapsed")
                                
                                if bk_action == "Chọn Booking có sẵn":
                                    if booking_choices:
                                        sel_b = st.selectbox("Chọn Booking:", list(booking_choices.keys()))
                                        selected_cost_code = booking_choices[sel_b]
                                    else:
                                        st.warning("Chưa có Tour nào đang chạy.")
                                        st.warning("Chưa có Booking nào.")
                                else:
                                    # Tự tạo mã Booking lẻ
                                    if "gen_booking_code" not in st.session_state:
                                        st.session_state.gen_booking_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                                    # Tạo mới Booking Dịch Vụ ngay tại đây
                                    c_new_b1, c_new_b2 = st.columns([1, 2])
                                    if "new_bk_code" not in st.session_state:
                                        st.session_state.new_bk_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                                    
                                    c_gen1, c_gen2 = st.columns([1, 3])
                                    c_gen1.text_input("Mã Booking:", value=st.session_state.gen_booking_code, disabled=True)
                                    if c_gen2.button("🔄 Tạo mã khác"):
                                        st.session_state.gen_booking_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                                        st.rerun()
                                    selected_cost_code = st.session_state.gen_booking_code
                                    new_bk_code = c_new_b1.text_input("Mã Booking (Tự động)", value=st.session_state.new_bk_code, disabled=True)
                                    new_bk_name = c_new_b2.text_input("Tên Booking / Dịch vụ", placeholder="VD: Khách lẻ A, Vé máy bay B...")
                                
                        else: # Hóa đơn
                            st.info("🔗 Liên kết chi phí")
                            inv_opt = st.radio("Nguồn gốc:", ["Theo mã UNC/Booking/Tour", "Không có UNC (Tự tạo mã)"], horizontal=True)
                            if inv_opt == "Theo mã UNC/Booking/Tour":
                                # Gộp cả mã Tour và mã Booking lẻ đã có
                                all_avail_codes = sorted(list(set(list(tour_choices.values()) + existing_codes)))
                                if all_avail_codes:
                                    selected_cost_code = st.selectbox("Chọn Mã liên kết:", all_avail_codes)
                                else:
                                    st.warning("Chưa có mã nào để liên kết.")
                            else:
                                if "gen_inv_code" not in st.session_state:
                                    st.session_state.gen_inv_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                                st.text_input("Mã chi phí mới:", value=st.session_state.gen_inv_code, disabled=True)
                                selected_cost_code = st.session_state.gen_inv_code
                                st.caption("Vui lòng nhập tên để tạo mã.")
                    
                    # Initialize variables to avoid unbound errors
                    txn_content = ""; seller = ""; buyer = ""

                    with st.form("inv_form"):
                        # Mặc định UNC là Đầu vào
                        default_idx = 0 
                        
                        # --- PHẦN 1: THÔNG TIN CHUNG ---
                        st.markdown("##### 📝 Thông tin chung")
                        with st.container(border=True):
                            st.text_input("Mã chi phí / Booking:", value=selected_cost_code, disabled=True)
                            st.divider()
                            
                            typ = st.radio("Loại", ["Đầu vào", "Đầu ra"], horizontal=True, index=default_idx)
                            drive_link = st.text_input("🔗 Link Drive (Tùy chọn)")
                            
                            c1, c2 = st.columns(2)
                            if doc_type == "Hóa đơn":
                                memo = st.text_input("Gợi nhớ (Memo)", value=d.get('file_name',''))
                                date = st.text_input("Ngày", value=d['date'])
                                num = c1.text_input("Số hóa đơn", value=d['inv_num'])
                                sym = c2.text_input("Ký hiệu/Mẫu số", value=d['inv_sym'])
                            else:
                                memo = c1.text_input("Gợi nhớ (Tên file)", value=d.get('file_name', ''))
                                date = c2.text_input("Ngày chuyển khoản", value=d['date'])
                                content_val = d.get('content', '')
                                txn_content = st.text_area("Nội dung chuyển khoản (OCR)", value=content_val, height=70)
                                num = ""; sym = ""; buyer = "" 
                        
                        # --- PHẦN 2: BÊN MUA / BÁN ---
                        if doc_type == "Hóa đơn" or doc_type == "Ủy nhiệm chi ":
                            st.markdown("##### 🤝 Đối tượng")
                            with st.container(border=True):
                                if doc_type == "Hóa đơn":
                                    seller = st.text_input("Bên Bán", value=d['seller'])
                                    buyer = st.text_input("Bên Mua", value=d['buyer'])
                                else:
                                    seller = st.text_input("Đơn vị nhận tiền", value=d['seller'])
                        
                        # --- PHẦN 3: TÀI CHÍNH ---
                        st.markdown("##### 💰 Tài chính")
                        with st.container(border=True):
                            if doc_type == "Hóa đơn":
                                pre = st.number_input("Tiền hàng", value=float(d['pre_tax']), disabled=st.session_state.edit_lock, format="%.0f")
                                tax = st.number_input("VAT", value=float(d['tax']), disabled=st.session_state.edit_lock, format="%.0f")
                                total = pre + tax
                            else:
                                st.caption("(Với UNC, chỉ cần nhập Số tiền đã chuyển nha)")
                                pre = 0; tax = 0
                                total = st.number_input("Số tiền đã chuyển", value=float(d['total']), disabled=st.session_state.edit_lock, format="%.0f")

                            is_locked_admin = False
                            # 3. & 5. LOGIC DUYỆT:
                            
                            if st.session_state.local_edit_count == 2:
                                st.markdown('<div style="background:#fff3cd; color:orange; padding:10px; border-radius:5px; margin-bottom:10px;">⚠️ <b>Lưu ý:</b> Nếu chỉnh sửa lần 3 phải gửi admin duyệt.</div>', unsafe_allow_html=True)
                            elif st.session_state.local_edit_count >= 3 and st.session_state.local_edit_count < 5:
                                is_locked_admin = True
                                st.markdown(f'<div style="background:#ffeef7; color:red; padding:10px; border-radius:5px; margin-bottom:10px;">🔒 <b>Chế độ duyệt:</b> Bạn đang sửa lần {st.session_state.local_edit_count}. Cần Admin duyệt.</div>', unsafe_allow_html=True)
                            elif st.session_state.local_edit_count >= 5:
                                st.error("⛔ Đã quá số lần chỉnh sửa cho phép (5 lần).")

                            # 6. HIỂN THỊ TIỀN 1 HÀNG (CSS .money-box đã xử lý)
                            st.write("") 
                            st.markdown(f'<div class="money-box">{format_vnd(total)}</div>', unsafe_allow_html=True)
                            
                            b1, b2 = st.columns(2)
                            
                            if st.session_state.local_edit_count < 5:
                                if b1.form_submit_button("✏️ Sửa giá"):
                                    st.session_state.edit_lock = False
                                    st.rerun()
                            
                            if not st.session_state.edit_lock and b2.form_submit_button("✅ Chốt giá"):
                                new_pre = pre if doc_type == "Hóa đơn" else total
                                st.session_state.pdf_data.update({'pre_tax': new_pre, 'tax': tax, 'total': total})
                                st.session_state.edit_lock = True
                                st.session_state.local_edit_count += 1
                                st.rerun()

                        # Nút Lưu / Gửi Duyệt
                        if is_locked_admin:
                            btn_label = "🚀 GỬI ADMIN DUYỆT"
                        elif st.session_state.local_edit_count >= 5:
                            btn_label = "⛔ ĐÃ KHÓA"
                        else:
                            btn_label = "💾 LƯU CHỨNG TỪ"
                        
                        if st.form_submit_button(btn_label, type="primary", width="stretch", disabled=(st.session_state.local_edit_count >= 5)):
                            if doc_type == "Hóa đơn" and (not date or not num): st.error("Ơ kìa, thiếu ngày hoặc số hóa đơn rồi!")
                            elif doc_type == "Ủy nhiệm chi " and not date: st.error("Thiếu ngày chuyển khoản rồi nè!")
                            elif not st.session_state.edit_lock: st.warning("Bấm 'Chốt giá' trước khi lưu nha!")
                            else:
                                # --- CHUẨN BỊ DỮ LIỆU ---
                                t = 'OUT' if "Đầu ra" in typ else 'IN'
                                save_memo = memo
                                save_num = num
                                
                                if doc_type == "Ủy nhiệm chi ":
                                    save_memo = f"[UNC] {memo} - {txn_content}"
                                    if not save_num: save_num = f"UNC-{datetime.now().strftime('%y%m%d%H%M')}"

                                # --- TẠO TÊN FILE ---
                                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                clean_name = re.sub(r'[\\/*?:"<>|]', "", uploaded_file.name)
                                final_name = f"{ts}_{clean_name}"
                                if st.session_state.ready_pdf_bytes and not final_name.lower().endswith('.pdf'):
                                    final_name = os.path.splitext(final_name)[0] + ".pdf"

                                # [CODE MỚI] 
                                # 1. Upload file lên Drive (Đã tắt theo yêu cầu - Chỉ lưu dữ liệu)
                                drive_link = ""
                                # if uploaded_file:
                                #     # Xử lý file upload (nếu là ảnh đã convert sang PDF thì dùng bytes)
                                #     if st.session_state.ready_pdf_bytes:
                                #         file_obj = io.BytesIO(st.session_state.ready_pdf_bytes)
                                #         drive_link = upload_to_drive(file_obj, final_name, mimetype='application/pdf')
                                #     else:
                                #         drive_link = upload_to_drive(uploaded_file, final_name)
                                
                                # 2. Chuẩn bị dữ liệu để lưu
                                new_invoice = {
                                    'type': t, 
                                    'date': date,
                                    'invoice_number': save_num,
                                    'invoice_symbol': sym,
                                    'seller_name': seller,
                                    'buyer_name': buyer,
                                    'pre_tax_amount': pre,
                                    'tax_amount': tax,
                                    'total_amount': total,
                                    'file_name': final_name,
                                    'status': 'active',
                                    'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    'memo': save_memo,
                                    'file_path': drive_link, 
                                    'cost_code': selected_cost_code,
                                    'edit_count': st.session_state.local_edit_count,
                                    'request_edit': 1 if is_locked_admin else 0
                                }
                                
                                # 3. Ghi vào Sheet 'invoices'
                                if add_row_to_table('invoices', new_invoice):
                                    st.success("Đã lưu thành công lên Cloud! 🎉")
                                    
                                    # Reset state
                                    time.sleep(1)
                                    st.session_state.pdf_data = None
                                    st.session_state.uploader_key += 1
                                    st.session_state.ready_pdf_bytes = None
                                    st.session_state.ready_file_name = None
                                    st.session_state.local_edit_count = 0
                                    if "gen_booking_code" in st.session_state: del st.session_state.gen_booking_code
                                    if "gen_inv_code" in st.session_state: del st.session_state.gen_inv_code
                                    if "new_bk_code" in st.session_state: del st.session_state.new_bk_code
                                    if "pending_booking_create" in st.session_state: del st.session_state.pending_booking_create
                                    st.rerun()

        st.divider()
        # --- 4. LỊCH SỬ NHẬP LIỆU (HIỆN TẤT CẢ NHƯNG CÓ NOTE) ---
        with st.expander("Lịch sử nhập liệu", expanded=True):
            rows = run_query("SELECT id, type, invoice_number, total_amount, status, memo, request_edit, edit_count, cost_code FROM invoices ORDER BY id DESC LIMIT 20")
            if rows:
                df = pd.DataFrame([dict(r) for r in rows])
                df['Chọn'] = False 
                
                def get_status_note(row): # type: ignore
                    if row['status'] == 'deleted': # type: ignore
                        return "❌ Đã xóa"
                    note = ""
                    if row['request_edit'] == 1: # type: ignore
                        note += "⏳ Chờ duyệt"
                    if row['edit_count'] > 0: # type: ignore
                        if note: note += " | "
                        note += f"✏️ Sửa {row['edit_count']} lần" # type: ignore
                    
                    if not note:
                        return "✅ Hoạt động"
                    return note.strip(" | ")
                
                df['Trạng thái'] = df.apply(get_status_note, axis=1)
                
                df = df[['Chọn', 'id', 'cost_code', 'type', 'invoice_number', 'total_amount', 'Trạng thái', 'memo']]
                df.columns = ['Chọn', 'ID', 'Mã Chi Phí', 'Loại', 'Số HĐ', 'Tổng Tiền', 'Trạng thái', 'Ghi chú']
                
                df['Tổng Tiền'] = df['Tổng Tiền'].apply(format_vnd)

                edited_df = st.data_editor(
                    df,
                    column_config={
                        "Chọn": st.column_config.CheckboxColumn(required=True),
                        "ID": st.column_config.NumberColumn(disabled=True),
                        "Mã Chi Phí": st.column_config.TextColumn(disabled=True),
                        "Loại": st.column_config.TextColumn(disabled=True),
                        "Số HĐ": st.column_config.TextColumn(disabled=True),
                        "Tổng Tiền": st.column_config.TextColumn(disabled=True),
                        "Trạng thái": st.column_config.TextColumn(disabled=True),
                        "Ghi chú": st.column_config.TextColumn(disabled=True),
                    },
                    hide_index=True,
                    use_container_width=True
                )

                if st.button("🗑️ Xóa các mục đã chọn", type="primary"):
                    selected_ids = edited_df[edited_df['Chọn']]['ID'].tolist()
                    if selected_ids:
                        for i in selected_ids:
                            run_query("UPDATE invoices SET status='deleted' WHERE id=?", (i,), commit=True)
                        st.success(f"Đã xóa {len(selected_ids)} hóa đơn!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.warning("Bạn chưa chọn mục nào cả.")
            else:
                st.info("Chưa có hóa đơn nào.")
    elif menu == "2. Báo Cáo Tổng Hợp":
        st.title("📊 Báo Cáo Tài Chính")

        all_financial_records = []
        with st.spinner("Đang tổng hợp dữ liệu từ tất cả các phân hệ..."):
            # --- OPTIMIZED DATA FETCHING ---
            # Lọc booking theo sale nếu cần
            user_info_rpt = st.session_state.get("user_info", {})
            user_role_rpt = user_info_rpt.get('role')
            user_name_rpt = user_info_rpt.get('name')

            # 1. Fetch all base data in a few queries
            tour_rpt_query = "SELECT * FROM tours WHERE status != 'deleted'"
            tour_rpt_params = []
            if user_role_rpt == 'sale' and user_name_rpt:
                tour_rpt_query += " AND sale_name=?"
                tour_rpt_params.append(user_name_rpt)
            all_tours = run_query(tour_rpt_query, tuple(tour_rpt_params))
            
            bk_rpt_query = "SELECT * FROM service_bookings WHERE status != 'deleted'"
            bk_rpt_params = []
            if user_role_rpt == 'sale' and user_name_rpt:
                bk_rpt_query += " AND sale_name=?"
                bk_rpt_params.append(user_name_rpt)
            all_bookings = run_query(bk_rpt_query, tuple(bk_rpt_params))

            all_linked_invoices = run_query("SELECT cost_code, type, invoice_number, total_amount FROM invoices WHERE status='active' AND request_edit=0 AND cost_code IS NOT NULL AND cost_code != ''")
            # [NEW] Fetch all transactions for debt calculation
            all_transactions = run_query("SELECT ref_code, type, amount FROM transaction_history")

            # 2. Process data in memory using dictionaries for fast lookups
            invoice_costs_by_code = {}
            for inv in all_linked_invoices:
                code = inv['cost_code']
                if code not in invoice_costs_by_code:
                    invoice_costs_by_code[code] = {'IN_INV': 0, 'IN_UNC': 0}
                if inv['type'] == 'IN':
                    is_unc = 'UNC' in (inv.get('invoice_number') or '') # type: ignore
                    if is_unc:
                        invoice_costs_by_code[code]['IN_UNC'] += inv['total_amount'] # type: ignore
                    else:
                        invoice_costs_by_code[code]['IN_INV'] += inv['total_amount'] # type: ignore
            
            # [NEW] Process transactions to get paid amounts
            paid_amounts = {}
            if all_transactions:
                df_txns = pd.DataFrame([dict(r) for r in all_transactions])
                if not df_txns.empty:
                    df_thu = df_txns[df_txns['type'] == 'THU'].groupby('ref_code')['amount'].sum()
                    df_chi = df_txns[df_txns['type'] == 'CHI'].groupby('ref_code')['amount'].sum() # CHI means refund
                    paid_amounts = (df_thu.subtract(df_chi, fill_value=0)).to_dict()

            # --- Process Tours ---
            if all_tours:
                for tour_row in all_tours:
                    tour = dict(tour_row)
                    # [NEW] Add status to record
                    tour_status = tour.get('status', 'running')
                    revenue, cost = get_tour_financials(tour['id'], tour)
                    if revenue > 0: all_financial_records.append({'date_str': tour['start_date'], 'name': tour['tour_name'], 'code': tour['tour_code'], 'category': 'Tour', 'type': 'thu', 'amount': revenue, 'status': tour_status}) # type: ignore
                    if cost > 0: all_financial_records.append({'date_str': tour['start_date'], 'name': tour['tour_name'], 'code': tour['tour_code'], 'category': 'Tour', 'type': 'chi', 'amount': cost, 'status': tour_status}) # type: ignore

            # --- Process Service Bookings ---
            if all_bookings:
                for booking_row in all_bookings:
                    booking = dict(booking_row)
                    
                    # [FIX] Chuyển đổi định dạng ngày YYYY-MM-DD sang DD/MM/YYYY để đồng bộ
                    try:
                        booking_date_obj = datetime.strptime(str(booking['created_at']).split(" ")[0], '%Y-%m-%d')
                        booking_date_str = booking_date_obj.strftime('%d/%m/%Y')
                    except:
                        booking_date_str = booking['created_at']
                    # [NEW] Add status to record
                    booking_status = booking.get('status', 'active')

                    if booking.get('selling_price', 0) > 0:
                        all_financial_records.append({'date_str': booking_date_str, 'name': booking['name'], 'code': booking['code'], 'category': 'Booking Dịch Vụ', 'type': 'thu', 'amount': booking['selling_price'], 'status': booking_status}) # type: ignore
                    
                    # [FIX] Chỉ tính chi phí từ hóa đơn (IN_INV), không tính UNC để tránh double-count.
                    # UNC là thanh toán cho chi phí, không phải bản thân chi phí.
                    total_cost_booking = invoice_costs_by_code.get(booking['code'], {}).get('IN_INV', 0)
                    if total_cost_booking == 0 and booking.get('net_price', 0) > 0:
                        total_cost_booking = booking['net_price'] # type: ignore
                    if total_cost_booking > 0:
                        all_financial_records.append({'date_str': booking_date_str, 'name': booking['name'], 'code': booking['code'], 'category': 'Booking Dịch Vụ', 'type': 'chi', 'amount': total_cost_booking, 'status': booking_status}) # type: ignore

            # --- Process old Projects & Unlinked Invoices (These queries are already efficient) ---
            project_invoices = run_query("SELECT p.project_name, i.type, i.total_amount, i.date, p.id as project_id FROM projects p JOIN project_links l ON p.id = l.project_id JOIN invoices i ON l.invoice_id = i.id WHERE i.status = 'active' AND i.request_edit = 0")
            if project_invoices:
                for inv in project_invoices:
                    all_financial_records.append({'date_str': inv['date'], 'name': inv['project_name'], 'code': f"PROJ_{inv['project_id']}", 'category': 'Dự án (cũ)', 'type': 'thu' if inv['type'] == 'OUT' else 'chi', 'amount': inv['total_amount'], 'status': 'N/A'}) # type: ignore

            unlinked_invoices = run_query("SELECT * FROM invoices i WHERE i.status = 'active' AND i.request_edit = 0 AND (i.cost_code IS NULL OR i.cost_code = '') AND NOT EXISTS (SELECT 1 FROM project_links pl WHERE pl.invoice_id = i.id)")
            if unlinked_invoices:
                for inv in unlinked_invoices:
                    all_financial_records.append({'date_str': inv['date'], 'name': inv['memo'] or inv['seller_name'] or 'Chi phí chung', 'code': f"INV_{inv['id']}", 'category': 'Chi phí chung', 'type': 'thu' if inv['type'] == 'OUT' else 'chi', 'amount': inv['total_amount'], 'status': 'N/A'}) # type: ignore

        if not all_financial_records:
            st.info("Chưa có dữ liệu tài chính để báo cáo.")
        else:
            df = pd.DataFrame(all_financial_records)
            df['date'] = pd.to_datetime(df['date_str'], errors='coerce', dayfirst=True)
            df['status'] = df['status'].fillna('N/A') # Đảm bảo cột status không có giá trị null
            df = df.dropna(subset=['date'])

            # Explicitly create a DatetimeIndex to help Pylance with type inference
            dt_index = pd.DatetimeIndex(df['date'])
            df['year'] = dt_index.year
            df['quarter'] = dt_index.quarter
            df['month_year'] = dt_index.to_period('M').astype(str)
            df['quarter_year'] = df.apply(lambda row: f"Q{row['quarter']}/{row['year']}", axis=1)

            st.markdown("####  Lọc báo cáo")
            c1, c2, c3 = st.columns(3)
            filter_type = c1.selectbox("Lọc theo thời gian:", ["Tháng", "Quý", "Năm"])
            
            options = []
            period_col = ''
            if filter_type == "Tháng":
                options = sorted(df['month_year'].unique(), reverse=True)
                period_col = 'month_year'
            elif filter_type == "Quý":
                options = sorted(df['quarter_year'].unique(), reverse=True)
                period_col = 'quarter_year'
            elif filter_type == "Năm":
                options = sorted(df['year'].unique(), reverse=True)
                period_col = 'year'
                
            selected_period = c2.selectbox(f"Chọn kỳ:", ["Tất cả"] + options)

            # [NEW] Thêm bộ lọc trạng thái
            status_map = {
                "Tất cả trạng thái": None,
                "Đang chạy / Hoạt động": ['running', 'active'],
                "Đã hoàn thành": ['completed']
            }
            selected_status_label = c3.selectbox("Lọc theo trạng thái:", list(status_map.keys()))
            selected_statuses = status_map[selected_status_label]

            # Áp dụng các bộ lọc
            df_filtered = df.copy()
            if selected_period != "Tất cả":
                df_filtered = df_filtered[df_filtered[period_col] == selected_period]
            
            if selected_statuses:
                # Chỉ lọc các mục có trạng thái (Tour/Booking), giữ lại các mục khác (Chi phí chung...)
                mask = df_filtered['status'].isin(selected_statuses) | (df_filtered['status'] == 'N/A')
                df_filtered = df_filtered[mask]

            if not df_filtered.empty:
                agg = df_filtered.pivot_table(index=['category', 'name', 'code'], columns='type', values='amount', aggfunc='sum').fillna(0)
                agg = agg.reset_index()
                
                if 'thu' not in agg.columns: agg['thu'] = 0
                if 'chi' not in agg.columns: agg['chi'] = 0
                agg['lợi nhuận'] = agg['thu'] - agg['chi']
                
                total_thu = agg['thu'].sum()
                total_chi = agg['chi'].sum()
                total_loi_nhuan = agg['lợi nhuận'].sum()
                
                m1, m2, m3 = st.columns(3)
                m1.metric(f"Tổng Thu ({selected_period})", format_vnd(total_thu))
                m2.metric(f"Tổng Chi ({selected_period})", format_vnd(total_chi))
                m3.metric(f"Lợi Nhuận ({selected_period})", format_vnd(total_loi_nhuan), delta=format_vnd(total_loi_nhuan) if total_loi_nhuan != 0 else None)

                st.divider()
                
                st.markdown("#### Chi tiết theo hạng mục")
                # Sort categories by total profit
                category_profit = agg.groupby('category')['lợi nhuận'].sum().sort_values(ascending=False)
                
                for category in category_profit.index:
                    group = agg[agg['category'] == category]
                    with st.expander(f"📂 {category} (Lợi nhuận: {format_vnd(group['lợi nhuận'].sum())})", expanded=True):
                        group = group.sort_values('lợi nhuận', ascending=False)
                        for _, r in group.iterrows():
                            # --- [NEW] Debt calculation & display ---
                            debt_html = ""
                            # Only calculate for Tours and Bookings which have revenue
                            if r['category'] in ['Tour', 'Booking Dịch Vụ'] and r['thu'] > 0:
                                code = r['code']
                                revenue = r['thu']
                                paid = paid_amounts.get(code, 0.0)
                                remaining = revenue - paid
                                
                                if remaining <= 0.1: # Use a small threshold for float comparison
                                    debt_html = f'''<div style="margin-top: 8px; font-size: 0.9em; text-align: right;">
                                        <span style="color: #2e7d32; font-weight: bold;">✅ Đã thanh toán đủ</span>
                                    </div>'''
                                else:
                                    debt_html = f'''<div style="margin-top: 8px; font-size: 0.9em; text-align: right;">
                                        <span style="color: #c62828; font-weight: bold;">Còn phải thu: {format_vnd(remaining)}</span>
                                    </div>'''
                            # --- End of new code ---

                            st.markdown(f"""
                            <div class="report-card" style="padding: 15px; margin-bottom: 10px; border-left: 5px solid {'#28a745' if r['lợi nhuận']>=0 else '#e53935'};">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 8px;">
                                    <h5 style="margin:0; padding-right: 10px;">{r['name']}</h5>
                                    <span style="font-size: 0.8em; color: #6c757d; background-color: #f1f3f5; padding: 2px 6px; border-radius: 5px; white-space: nowrap;">CODE: {r['code']}</span>
                                </div>
                                <div style="display:flex; justify-content:space-between; font-size: 0.95em; border-bottom: 1px solid #f1f3f5; padding-bottom: 8px;">
                                    <span>Thu: <b>{format_vnd(r['thu'])}</b></span>
                                    <span>Chi: <b>{format_vnd(r['chi'])}</b></span>
                                    <span style="font-weight: bold; color:{'#1B5E20' if r['lợi nhuận']>=0 else '#c62828'}">Lãi: {format_vnd(r['lợi nhuận'])}</span>
                                </div>
                                {debt_html}
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.info(f"Không có dữ liệu cho kỳ báo cáo '{selected_period}'.")

def render_debt_management():
    st.title("💳 Quản Lý Công Nợ")
    st.caption("Theo dõi và tổng hợp các khoản phải thu từ khách hàng.")

    tab_lookup, tab_summary = st.tabs(["Tra cứu theo Mã", "Tổng hợp Công nợ"])

    with tab_lookup:
        st.subheader("Tra cứu công nợ theo Mã Tour / Booking")
        
        # --- LẤY DỮ LIỆU ĐỂ TÌM KIẾM (CHỈ HIỆN CÁC MÃ CÒN NỢ) ---
        with st.spinner("Đang tải danh sách còn nợ..."):
            # 1. Lấy tất cả giao dịch và tính toán số tiền đã trả cho mỗi mã
            all_txns_cn = run_query("SELECT ref_code, type, amount FROM transaction_history")
            paid_amounts_cn = {}
            if all_txns_cn:
                df_txns_cn = pd.DataFrame([dict(r) for r in all_txns_cn])
                if not df_txns_cn.empty:
                    df_thu_cn = df_txns_cn[df_txns_cn['type'] == 'THU'].groupby('ref_code')['amount'].sum()
                    df_chi_cn = df_txns_cn[df_txns_cn['type'] == 'CHI'].groupby('ref_code')['amount'].sum()
                    paid_amounts_cn = (df_thu_cn.subtract(df_chi_cn, fill_value=0)).to_dict()

            # 2. Lấy tất cả tour và booking (lọc theo sale nếu cần)
            user_info_cn = st.session_state.get("user_info", {})
            user_role_cn = user_info_cn.get('role')
            user_name_cn = user_info_cn.get('name')

            # [FIX] Lấy tất cả tour/booking chưa bị xóa (bao gồm cả mục đã hoàn thành) để kiểm tra công nợ
            tour_cn_query = "SELECT * FROM tours WHERE COALESCE(status, 'running') NOT IN ('deleted')"
            tour_cn_params = []
            if user_role_cn == 'sale' and user_name_cn:
                tour_cn_query += " AND sale_name=?"
                tour_cn_params.append(user_name_cn)
            all_tours_cn = run_query(tour_cn_query, tuple(tour_cn_params))

            bk_cn_query = "SELECT * FROM service_bookings WHERE COALESCE(status, 'active') NOT IN ('deleted')"
            bk_cn_params = []
            if user_role_cn == 'sale' and user_name_cn:
                bk_cn_query += " AND sale_name=?"
                bk_cn_params.append(user_name_cn)
            all_bookings_cn = run_query(bk_cn_query, tuple(bk_cn_params))

            search_options = {"": "-- Chọn mã để theo dõi --"}

            # 3. Xử lý Tours: Chỉ thêm vào danh sách nếu chưa thu đủ
            if all_tours_cn:
                for t_row in all_tours_cn:
                    tour = dict(t_row)
                    # Tính giá trị hợp đồng
                    final_price = float(tour.get('final_tour_price', 0) or 0)
                    child_price = float(tour.get('child_price', 0) or 0)
                    final_qty = float(tour.get('final_qty', 0) or 0)
                    child_qty = float(tour.get('child_qty', 0) or 0)
                    if final_qty == 0: final_qty = float(tour.get('guest_count', 1))
                    contract_value = (final_price * final_qty) + (child_price * child_qty)
                    
                    paid = paid_amounts_cn.get(tour['tour_code'], 0.0)
                    
                    if contract_value > 0 and contract_value - paid > 0.1:
                        search_options[f"📦 TOUR: [{tour['tour_code']}] {tour['tour_name']}"] = tour['tour_code']

            # 4. Xử lý Bookings: Chỉ thêm vào danh sách nếu chưa thu đủ
            if all_bookings_cn:
                for b_row in all_bookings_cn:
                    booking = dict(b_row)
                    contract_value = float(booking.get('selling_price', 0) or 0)
                    paid = paid_amounts_cn.get(booking['code'], 0.0)
                    if contract_value > 0 and contract_value - paid > 0.1:
                        search_options[f"🔖 BOOKING: [{booking['code']}] {booking['name']}"] = booking['code']

        # --- GIAO DIỆN CHÍNH ---
        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown("#### 🔍 Chọn đối tượng")
            selected_label = st.selectbox("Tìm theo Mã Tour / Booking (chỉ hiện mã còn nợ):", list(search_options.keys()), label_visibility="collapsed")
            selected_code = search_options.get(selected_label)

            if selected_code:
                st.markdown("---")
                st.markdown("#### 📊 Tổng quan công nợ")

                contract_value = 0.0
                # Xác định giá trị hợp đồng
                if "TOUR" in selected_label:
                    tour_info = run_query("SELECT * FROM tours WHERE tour_code=?", (selected_code,), fetch_one=True)
                    if tour_info:
                        t_dict = dict(tour_info)
                        final_price = float(t_dict.get('final_tour_price', 0) or 0)
                        child_price = float(t_dict.get('child_price', 0) or 0)
                        final_qty = float(t_dict.get('final_qty', 0) or 0)
                        child_qty = float(t_dict.get('child_qty', 0) or 0)
                        if final_qty == 0: final_qty = float(t_dict.get('guest_count', 1))
                        contract_value = (final_price * final_qty) + (child_price * child_qty)
                elif "BOOKING" in selected_label:
                    booking_info = run_query("SELECT selling_price FROM service_bookings WHERE code=?", (selected_code,), fetch_one=True)
                    if booking_info:
                        contract_value = float(booking_info['selling_price'] or 0)

                # Lấy tổng đã thu
                paid_data = run_query("SELECT SUM(amount) as total FROM transaction_history WHERE ref_code=? AND type='THU'", (selected_code,), fetch_one=True)
                total_paid = paid_data['total'] if paid_data and paid_data['total'] else 0.0

                # Lấy tổng đã chi (hoàn tiền)
                refund_data = run_query("SELECT SUM(amount) as total FROM transaction_history WHERE ref_code=? AND type='CHI'", (selected_code,), fetch_one=True)
                total_refund = refund_data['total'] if refund_data and refund_data['total'] else 0.0
                
                actual_paid = total_paid - total_refund
                
                remaining = contract_value - actual_paid

                with st.container(border=True):
                    st.metric("Giá trị Hợp đồng/Booking", format_vnd(contract_value))
                    st.metric("Đã thu thực tế", format_vnd(actual_paid))
                    delta_color = "inverse" if remaining > 0 else "off"
                    st.metric("Còn phải thu", format_vnd(remaining), delta=f"-{format_vnd(remaining)}" if remaining > 0 else "✅ Đã thu đủ", delta_color=delta_color)

        with col2:
            if selected_code:
                tab_add, tab_history = st.tabs(["➕ Tạo Phiếu Thu/Chi", "📜 Lịch sử giao dịch"])

                with tab_add:
                    st.markdown("##### Tạo phiếu mới")
                    with st.form(f"add_txn_{selected_code}", clear_on_submit=True):
                        c1, c2 = st.columns(2)
                        txn_type = c1.radio("Loại phiếu", ["THU", "CHI (Hoàn tiền)"], horizontal=True)
                        txn_amount = c2.number_input("Số tiền", min_value=0.0, format="%.0f")
                        
                        c3, c4 = st.columns(2)
                        txn_method = c3.selectbox("Hình thức", ["Chuyển khoản", "Tiền mặt"])
                        txn_note = c4.text_input("Nội dung", placeholder="VD: Cọc lần 1, Thanh toán...")
                        
                        if st.form_submit_button("💾 Lưu Phiếu", type="primary", use_container_width=True):
                            if txn_amount > 0 and txn_note:
                                run_query(
                                    "INSERT INTO transaction_history (ref_code, type, amount, payment_method, note, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                                    (selected_code, txn_type, txn_amount, txn_method, txn_note, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                                    commit=True
                                )
                                st.success("Đã lưu phiếu thành công!")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.warning("Vui lòng nhập số tiền và nội dung.")

                with tab_history:
                    st.markdown("##### Lịch sử các lần thanh toán")
                    history = run_query("SELECT * FROM transaction_history WHERE ref_code=? ORDER BY id DESC", (selected_code,))
                    
                    if history:
                        df_hist = pd.DataFrame([dict(r) for r in history])
                        df_hist['Xóa'] = False
                        df_hist = df_hist[['Xóa', 'id', 'created_at', 'type', 'amount', 'payment_method', 'note']]
                        
                        edited_df = st.data_editor(
                            df_hist,
                            column_config={
                                "Xóa": st.column_config.CheckboxColumn(required=True),
                                "id": st.column_config.NumberColumn(disabled=True),
                                "created_at": st.column_config.TextColumn("Ngày tạo", disabled=True),
                                "type": st.column_config.TextColumn("Loại", disabled=True),
                                "amount": st.column_config.NumberColumn("Số tiền", format="%d", disabled=True),
                                "payment_method": st.column_config.TextColumn("Hình thức", disabled=True),
                                "note": st.column_config.TextColumn("Nội dung", disabled=True),
                            },
                            hide_index=True,
                            use_container_width=True,
                            key=f"history_editor_{selected_code}",
                        )
                        
                        if st.button("🗑️ Xóa các phiếu đã chọn", type="secondary", key=f"delete_txn_{selected_code}"):
                            selected_ids = edited_df[edited_df['Xóa']]['id'].tolist()
                            if selected_ids:
                                for i in selected_ids:
                                    run_query("DELETE FROM transaction_history WHERE id=?", (i,), commit=True)
                                st.success(f"Đã xóa {len(selected_ids)} phiếu!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.warning("Bạn chưa chọn phiếu nào để xóa.")
                    else:
                        st.info("Chưa có lịch sử giao dịch cho mã này.")
            else:
                st.info("👆 Vui lòng chọn một Mã Tour hoặc Mã Booking để xem công nợ.")

    with tab_summary:
        st.subheader("Tổng hợp các khoản phải thu")
        with st.spinner("Đang tính toán công nợ..."):
            # 1. Lấy tất cả giao dịch và tính toán số tiền đã trả cho mỗi mã
            all_txns = run_query("SELECT ref_code, type, amount FROM transaction_history")
            paid_amounts = {}
            if all_txns:
                df_txns = pd.DataFrame([dict(r) for r in all_txns])
                if not df_txns.empty:
                    df_thu = df_txns[df_txns['type'] == 'THU'].groupby('ref_code')['amount'].sum()
                    df_chi = df_txns[df_txns['type'] == 'CHI'].groupby('ref_code')['amount'].sum()
                    paid_amounts = (df_thu.subtract(df_chi, fill_value=0)).to_dict()

            debt_records = []

            # 2. Lấy tất cả tour đang hoạt động và tính công nợ
            user_info_debt = st.session_state.get("user_info", {})
            user_role_debt = user_info_debt.get('role')
            user_name_debt = user_info_debt.get('name')
            # [FIX] Lấy tất cả tour chưa bị xóa (bao gồm cả tour đã hoàn thành) để tổng hợp công nợ
            tour_debt_query = "SELECT * FROM tours WHERE COALESCE(status, 'running') NOT IN ('deleted')"
            tour_debt_params = []
            if user_role_debt == 'sale' and user_name_debt:
                tour_debt_query += " AND sale_name=?"
                tour_debt_params.append(user_name_debt)
            active_tours = run_query(tour_debt_query, tuple(tour_debt_params))
            if active_tours:
                for tour_row in active_tours:
                    tour = dict(tour_row)
                    final_price = float(tour.get('final_tour_price', 0) or 0)
                    child_price = float(tour.get('child_price', 0) or 0)
                    final_qty = float(tour.get('final_qty', 0) or 0)
                    child_qty = float(tour.get('child_qty', 0) or 0)
                    if final_qty == 0: final_qty = float(tour.get('guest_count', 1))
                    contract_value = (final_price * final_qty) + (child_price * child_qty)
 
                    if contract_value > 0:
                        paid = paid_amounts.get(tour['tour_code'], 0.0)
                        remaining = contract_value - paid
                        if remaining > 0.1:
                            debt_records.append({'customer_name': tour.get('customer_name', 'N/A'), 'ref_name': tour['tour_name'], 'ref_code': tour['tour_code'], 'type': 'Tour', 'contract_value': contract_value, 'paid': paid, 'remaining': remaining})
 
            # 3. Lấy tất cả booking lẻ đang hoạt động và tính công nợ
            # [FIX] Lấy tất cả booking chưa bị xóa (bao gồm cả booking đã hoàn thành) để tổng hợp công nợ
            bk_debt_query = "SELECT * FROM service_bookings WHERE COALESCE(status, 'active') NOT IN ('deleted')"
            bk_debt_params = []
            if user_role_debt == 'sale' and user_name_debt:
                bk_debt_query += " AND sale_name=?"
                bk_debt_params.append(user_name_debt)
            active_bookings = run_query(bk_debt_query, tuple(bk_debt_params))
            if active_bookings:
                for booking_row in active_bookings:
                    booking = dict(booking_row)
                    contract_value = float(booking.get('selling_price', 0) or 0)
 
                    if contract_value > 0:
                        paid = paid_amounts.get(booking['code'], 0.0)
                        remaining = contract_value - paid
                        if remaining > 0.1:
                            customer_info = booking.get('customer_info', 'N/A')
                            customer_name = customer_info.split(' - ')[0] if ' - ' in customer_info else customer_info
                            debt_records.append({'customer_name': customer_name, 'ref_name': booking['name'], 'ref_code': booking['code'], 'type': 'Booking', 'contract_value': contract_value, 'paid': paid, 'remaining': remaining})
 
            # 4. Hiển thị kết quả
            if not debt_records:
                st.success("🎉 Không có công nợ nào cần thu.")
            else:
                df_debt = pd.DataFrame(debt_records)
                total_debt = df_debt['remaining'].sum()
                
                st.metric("TỔNG SỐ TIỀN CẦN THU", format_vnd(total_debt))
                
                st.divider()
                st.markdown("#### Danh sách khách hàng đang nợ")
                
                customer_debt = df_debt.groupby('customer_name')['remaining'].sum().reset_index().sort_values('remaining', ascending=False)
                customer_debt.columns = ['Khách hàng', 'Tổng nợ']
                
                st.dataframe(customer_debt, column_config={"Tổng nợ": st.column_config.NumberColumn(format="%d VND")}, use_container_width=True, hide_index=True)
                
                st.divider()
                st.markdown("#### Chi tiết các khoản nợ")
                st.dataframe(
                    df_debt.sort_values('remaining', ascending=False),
                    column_config={ 'customer_name': 'Khách hàng', 'ref_name': 'Tên Tour/Booking', 'ref_code': 'Mã', 'type': 'Loại', 'contract_value': st.column_config.NumberColumn("Giá trị HĐ", format="%d VND"), 'paid': st.column_config.NumberColumn("Đã thu", format="%d VND"), 'remaining': st.column_config.NumberColumn("Còn lại", format="%d VND"), },
                    use_container_width=True, hide_index=True
                )

def render_booking_management():
    st.title("🔖 Quản Lý Booking")
    st.caption("Quản lý các booking lẻ, booking dịch vụ (Không phải Tour trọn gói)")
    
    # Lấy thông tin user hiện tại để gán cho booking và lọc dữ liệu
    current_user_info = st.session_state.get("user_info", {})
    current_user_name = current_user_info.get('name', 'N/A')
    current_user_role = current_user_info.get('role')

    # --- 2. TÁCH LIÊN KẾT RA 2 PHẦN RIÊNG BIỆT ---
    tab1, tab2, tab3 = st.tabs(["✨ Tạo Booking", "🔗 Chi tiết Booking", "📜 Lịch sử Booking"])
    
    # ---------------- TAB 1: TẠO BOOKING ----------------
    with tab1:
        with st.container(border=True):
            st.markdown("### ➕ Tạo Booking Mới")
            
            # --- GỢI Ý KHÁCH HÀNG ---
            cust_query = "SELECT * FROM customers ORDER BY id DESC"
            cust_params = []
            if current_user_role == 'sale' and current_user_name:
                cust_query = "SELECT * FROM customers WHERE sale_name=? ORDER BY id DESC"
                cust_params.append(current_user_name)
            customers = run_query(cust_query, tuple(cust_params))
            cust_opts = ["-- Khách mới --"] + [f"{c['name']} | {c['phone']}" for c in customers] if customers else ["-- Khách mới --"] # type: ignore
            sel_cust = st.selectbox("🔍 Chọn khách hàng cũ (Gợi ý):", cust_opts, key="bk_cust_suggest")
            
            pre_name, pre_phone = "", ""
            if sel_cust and sel_cust != "-- Khách mới --":
                parts = sel_cust.split(" | ")
                pre_name = parts[0]
                pre_phone = parts[1] if len(parts) > 1 else ""
            
            # Chọn loại dịch vụ
            bk_type = st.radio("Chọn loại dịch vụ:", ["🏨 Khách sạn", "🚌 Vận chuyển", "🧩 Combo / Đa dịch vụ", "🔖 Khác"], horizontal=True)
            st.divider()

            if bk_type == "🏨 Khách sạn":
                st.markdown("##### 💰 Thông tin tài chính")
                f1, f2 = st.columns(2)
                net_price = f1.number_input("Giá nét", min_value=0.0, format="%.0f")
                selling_price = f2.number_input("Giá bán", min_value=0.0, format="%.0f")
                
                tax_option = st.radio("Giá nét đã bao gồm thuế?", ["Đã bao gồm thuế", "Chưa bao gồm thuế"], horizontal=True)
                tax_percent = 0.0
                net_price_incl_tax = net_price
                
                if tax_option == "Chưa bao gồm thuế":
                    tax_percent = st.number_input("Nhập % Thuế", min_value=0.0, max_value=100.0, step=0.5, format="%.1f")
                    net_price_incl_tax = net_price * (1 + tax_percent / 100)
                    st.info(f"Giá nét bao gồm thuế: **{format_vnd(net_price_incl_tax)}**")

                profit = selling_price - net_price_incl_tax
                st.metric("Lợi nhuận dự kiến", f"{format_vnd(profit)} VND")
                st.divider()
                st.text_input("Sales phụ trách", value=current_user_name, disabled=True)
                with st.form("bk_hotel", clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    h_name = c1.text_input("Tên Khách sạn", placeholder="VD: Mường Thanh Luxury")
                    dates = c2.date_input("Thời gian lưu trú", value=[], help="Chọn ngày nhận và trả phòng", format="DD/MM/YYYY")
                    
                    c_cust_n, c_cust_p = st.columns(2)
                    cust_name = c_cust_n.text_input("Tên khách hàng (*)", value=pre_name, placeholder="Nhập tên khách")
                    cust_phone = c_cust_p.text_input("Số điện thoại", value=pre_phone, placeholder="Nhập SĐT (Tùy chọn)")

                    new_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                    st.caption(f"Mã Booking dự kiến: {new_code}")
                    if st.form_submit_button("Tạo Booking Khách sạn", type="primary"):
                        if h_name and len(dates) == 2 and cust_name:
                            cust_info = f"{cust_name} - {cust_phone}" if cust_phone else cust_name
                            nights = (dates[1] - dates[0]).days
                            d_range = f"{dates[0].strftime('%d/%m/%Y')} - {dates[1].strftime('%d/%m/%Y')} ({nights} đêm)"
                            save_customer_check(cust_name, cust_phone, current_user_name)

                            add_row_to_table('service_bookings', {
                                'code': new_code, 'name': f"[KS] {h_name}", 'created_at': datetime.now().strftime("%Y-%m-%d"),
                                'type': 'HOTEL', 'details': f"Lưu trú: {d_range}", 'customer_info': cust_info,
                                'net_price': net_price_incl_tax,
                                'tax_percent': tax_percent,
                                'selling_price': selling_price,
                                'profit': profit,
                                'sale_name': current_user_name
                            })
                            st.success("Đã tạo!"); time.sleep(0.5); st.rerun()
                        else: st.warning("Vui lòng nhập tên khách sạn, tên khách hàng và chọn đủ ngày đi/về.")

            elif bk_type == "🚌 Vận chuyển":
                trans_type = st.radio("Loại phương tiện:", ["Xe (Ô tô)", "Máy bay", "Tàu hỏa"], horizontal=True)
                
                st.divider()
                st.markdown("##### 💰 Thông tin tài chính")
                f1, f2 = st.columns(2)
                net_price = f1.number_input("Giá nét", min_value=0.0, format="%.0f", key="trans_net")
                selling_price = f2.number_input("Giá bán", min_value=0.0, format="%.0f", key="trans_sell")
                
                tax_option = st.radio("Giá nét đã bao gồm thuế?", ["Đã bao gồm thuế", "Chưa bao gồm thuế"], horizontal=True, key="trans_tax_opt")
                tax_percent = 0.0
                net_price_incl_tax = net_price
                
                if tax_option == "Chưa bao gồm thuế":
                    tax_percent = st.number_input("Nhập % Thuế", min_value=0.0, max_value=100.0, step=0.5, format="%.1f", key="trans_tax_pct")
                    net_price_incl_tax = net_price * (1 + tax_percent / 100)
                    st.info(f"Giá nét bao gồm thuế: **{format_vnd(net_price_incl_tax)}**")

                profit = selling_price - net_price_incl_tax
                st.metric("Lợi nhuận dự kiến", f"{format_vnd(profit)} VND")
                st.divider()
                st.text_input("Sales phụ trách", value=current_user_name, disabled=True, key="trans_sale")
                with st.form("bk_trans", clear_on_submit=True):
                    details = ""
                    bk_name = ""
                    is_valid = False

                    if trans_type == "Xe (Ô tô)":
                        c1, c2 = st.columns(2)
                        route_from = c1.text_input("Điểm đi")
                        route_to = c2.text_input("Điểm đến")
                        c3, c4, c5 = st.columns(3)
                        car_type = c3.selectbox("Loại xe", ["4S", "7S", "16S", "29S", "35S", "45S"])
                        car_no = c4.text_input("Biển số / Mã xe")
                        t_date = c5.date_input("Ngày đi", format="DD/MM/YYYY")
                        
                        if route_from and route_to:
                            is_valid = True
                            bk_name = f"[XE] {route_from} - {route_to}"
                            details = f"Xe {car_type}: {car_no} | Ngày: {t_date.strftime('%d/%m/%Y')}"

                    elif trans_type == "Máy bay":
                        c1, c2 = st.columns(2)
                        ticket_code = c1.text_input("Mã vé / Số hiệu")
                        flight_date = c2.date_input("Ngày bay", format="DD/MM/YYYY")
                        flight_route = st.text_input("Hành trình / Hãng bay (Tùy chọn)", placeholder="VD: VN123 HAN-SGN")
                        
                        if ticket_code:
                            is_valid = True
                            desc = flight_route if flight_route else ticket_code
                            bk_name = f"[BAY] {desc}"
                            details = f"Vé: {ticket_code} | Ngày: {flight_date.strftime('%d/%m/%Y')}"

                    elif trans_type == "Tàu hỏa":
                        c1, c2 = st.columns(2)
                        ticket_code = c1.text_input("Mã vé / Toa / Ghế")
                        train_date = c2.date_input("Ngày đi", format="DD/MM/YYYY")
                        train_route = st.text_input("Ga đi - Ga đến (Tùy chọn)", placeholder="VD: Hà Nội - Vinh")
                        
                        if ticket_code:
                            is_valid = True
                            desc = train_route if train_route else ticket_code
                            bk_name = f"[TAU] {desc}"
                            details = f"Vé: {ticket_code} | Ngày: {train_date.strftime('%d/%m/%Y')}"

                    st.divider()
                    c_cust_n, c_cust_p = st.columns(2)
                    cust_name = c_cust_n.text_input("Tên khách hàng (*)", value=pre_name, placeholder="Nhập tên khách")
                    cust_phone = c_cust_p.text_input("Số điện thoại", value=pre_phone, placeholder="Nhập SĐT (Tùy chọn)")

                    new_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                    st.caption(f"Mã Booking dự kiến: {new_code}")
                    if st.form_submit_button("Tạo Booking Vận chuyển", type="primary"):
                        if is_valid and cust_name:
                            cust_info = f"{cust_name} - {cust_phone}" if cust_phone else cust_name
                            save_customer_check(cust_name, cust_phone, current_user_name)
                            add_row_to_table('service_bookings', {
                                'code': new_code, 'name': bk_name, 'created_at': datetime.now().strftime("%Y-%m-%d"),
                                'type': 'TRANS', 'details': details, 'customer_info': cust_info,
                                'net_price': net_price_incl_tax,
                                'tax_percent': tax_percent,
                                'selling_price': selling_price, 'profit': profit,
                                'sale_name': current_user_name
                            })
                            st.success("Đã tạo!"); time.sleep(0.5); st.rerun()
                        else: st.warning("Vui lòng nhập đủ thông tin (Hành trình/Mã vé và Tên khách).")

            elif bk_type == "🧩 Combo / Đa dịch vụ":
                if "combo_list" not in st.session_state: st.session_state.combo_list = []
                c_add, c_list = st.columns([1, 1.5])
                with c_add:
                    st.markdown("##### Thêm dịch vụ con")
                    sub_type = st.selectbox("Loại", ["Khách sạn", "Vận chuyển", "Khác"], key="cb_sub")
                    if sub_type == "Khách sạn":
                        sh_n = st.text_input("Tên KS", key="cb_h_n")
                        sh_d = st.date_input("Ngày ở", [], key="cb_h_d", format="DD/MM/YYYY")
                        if st.button("Thêm KS") and sh_n and len(sh_d)==2:
                            st.session_state.combo_list.append(f"🏨 {sh_n} ({sh_d[0].strftime('%d/%m')} - {sh_d[1].strftime('%d/%m')})"); st.rerun()
                    elif sub_type == "Vận chuyển":
                        st_r = st.text_input("Hành trình", key="cb_t_r")
                        st_d = st.date_input("Ngày", key="cb_t_d", format="DD/MM/YYYY")
                        if st.button("Thêm Xe") and st_r:
                            st.session_state.combo_list.append(f"🚌 {st_r} ({st_d.strftime('%d/%m')})"); st.rerun()
                    else:
                        so_n = st.text_input("Tên dịch vụ", key="cb_o_n")
                        if st.button("Thêm DV") and so_n:
                            st.session_state.combo_list.append(f"🔖 {so_n}"); st.rerun()
                with c_list:
                    st.markdown("##### Danh sách đã thêm")
                    for i, item in enumerate(st.session_state.combo_list): st.text(f"{i+1}. {item}")
                    if st.session_state.combo_list and st.button("Xóa hết", type="secondary"): st.session_state.combo_list = []; st.rerun()
                
                st.divider()
                st.markdown("##### 💰 Thông tin tài chính")
                f1, f2 = st.columns(2)
                net_price = f1.number_input("Giá nét", min_value=0.0, format="%.0f", key="combo_net")
                selling_price = f2.number_input("Giá bán", min_value=0.0, format="%.0f", key="combo_sell")
                
                tax_option = st.radio("Giá nét đã bao gồm thuế?", ["Đã bao gồm thuế", "Chưa bao gồm thuế"], horizontal=True, key="combo_tax_opt")
                tax_percent = 0.0
                net_price_incl_tax = net_price
                
                if tax_option == "Chưa bao gồm thuế":
                    tax_percent = st.number_input("Nhập % Thuế", min_value=0.0, max_value=100.0, step=0.5, format="%.1f", key="combo_tax_pct")
                    net_price_incl_tax = net_price * (1 + tax_percent / 100)
                    st.info(f"Giá nét bao gồm thuế: **{format_vnd(net_price_incl_tax)}**")

                profit = selling_price - net_price_incl_tax
                st.metric("Lợi nhuận dự kiến", f"{format_vnd(profit)} VND")
                st.divider()
                st.text_input("Sales phụ trách", value=current_user_name, disabled=True, key="combo_sale")
                with st.form("bk_combo", clear_on_submit=True):
                    combo_name = st.text_input("Tên Combo / Gói", placeholder="VD: Combo Đà Nẵng 3N2Đ")
                    c_cust_n, c_cust_p = st.columns(2)
                    cust_name = c_cust_n.text_input("Tên khách hàng (*)", value=pre_name, placeholder="Nhập tên khách")
                    cust_phone = c_cust_p.text_input("Số điện thoại", value=pre_phone, placeholder="Nhập SĐT (Tùy chọn)")

                    new_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                    if st.form_submit_button("Lưu Combo", type="primary"):
                        if combo_name and st.session_state.combo_list and cust_name:
                            cust_info = f"{cust_name} - {cust_phone}" if cust_phone else cust_name
                            save_customer_check(cust_name, cust_phone, current_user_name)
                            add_row_to_table('service_bookings', {
                                'code': new_code, 'name': f"[CB] {combo_name}", 'created_at': datetime.now().strftime("%Y-%m-%d"),
                                'type': 'COMBO', 'details': " | ".join(st.session_state.combo_list), 'customer_info': cust_info,
                                'net_price': net_price_incl_tax,
                                'tax_percent': tax_percent,
                                'selling_price': selling_price, 'profit': profit,
                                'sale_name': current_user_name
                            })
                            st.session_state.combo_list = []; st.success("Đã tạo!"); time.sleep(0.5); st.rerun()
                        else: st.warning("Cần tên Combo, tên khách hàng và ít nhất 1 dịch vụ.")

            else:
                st.markdown("##### 💰 Thông tin tài chính")
                f1, f2 = st.columns(2)
                net_price = f1.number_input("Giá nét", min_value=0.0, format="%.0f", key="other_net")
                selling_price = f2.number_input("Giá bán", min_value=0.0, format="%.0f", key="other_sell")
                
                tax_option = st.radio("Giá nét đã bao gồm thuế?", ["Đã bao gồm thuế", "Chưa bao gồm thuế"], horizontal=True, key="other_tax_opt")
                tax_percent = 0.0
                net_price_incl_tax = net_price
                
                if tax_option == "Chưa bao gồm thuế":
                    tax_percent = st.number_input("Nhập % Thuế", min_value=0.0, max_value=100.0, step=0.5, format="%.1f", key="other_tax_pct")
                    net_price_incl_tax = net_price * (1 + tax_percent / 100)
                    st.info(f"Giá nét bao gồm thuế: **{format_vnd(net_price_incl_tax)}**")

                profit = selling_price - net_price_incl_tax
                st.metric("Lợi nhuận dự kiến", f"{format_vnd(profit)} VND")
                st.divider()
                st.text_input("Sales phụ trách", value=current_user_name, disabled=True, key="other_sale")
                with st.form("bk_other", clear_on_submit=True):
                    new_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                    c1, c2 = st.columns([1, 3])
                    c1.text_input("Mã (Auto)", value=new_code, disabled=True)
                    new_name = c2.text_input("Tên Booking / Dịch vụ")
                    
                    c_cust_n, c_cust_p = st.columns(2)
                    cust_name = c_cust_n.text_input("Tên khách hàng (*)", value=pre_name, placeholder="Nhập tên khách")
                    cust_phone = c_cust_p.text_input("Số điện thoại", value=pre_phone, placeholder="Nhập SĐT (Tùy chọn)")

                    if st.form_submit_button("Tạo"):
                        if new_name and cust_name:
                            cust_info = f"{cust_name} - {cust_phone}" if cust_phone else cust_name
                            save_customer_check(cust_name, cust_phone, current_user_name)
                            add_row_to_table('service_bookings', {
                                'code': new_code, 'name': new_name, 'created_at': datetime.now().strftime("%Y-%m-%d"),
                                'type': 'OTHER', 'customer_info': cust_info,
                                'net_price': net_price_incl_tax,
                                'tax_percent': tax_percent,
                                'selling_price': selling_price, 'profit': profit,
                                'sale_name': current_user_name
                            })
                            st.success("Đã tạo!"); time.sleep(0.5); st.rerun()
                        else: st.warning("Vui lòng nhập tên dịch vụ và tên khách hàng.")

    # ---------------- TAB 2: KHỚP UNC & HÓA ĐƠN (DỰ ÁN UNC) ----------------
    with tab2:
        st.subheader("🔗 Chi tiết Booking")
        # --- Lọc danh sách booking theo sale ---
        bk_query = "SELECT * FROM service_bookings WHERE status='active'"
        bk_params = []
        if current_user_role == 'sale' and current_user_name:
            bk_query += " AND sale_name=?"
            bk_params.append(current_user_name)
        bk_query += " ORDER BY id DESC"
        bookings = run_query(bk_query, tuple(bk_params))
        
        if bookings:
            bk_map = {f"[{b['code']}] {b['name']}": b['code'] for b in bookings} # type: ignore
            selected_bk_label = st.selectbox("Chọn Booking để xem chi tiết:", list(bk_map.keys()))
            
            if selected_bk_label:
                code = bk_map[selected_bk_label] # type: ignore
                
                bk_info = run_query("SELECT * FROM service_bookings WHERE code=?", (code,), fetch_one=True)
                st.divider()
                st.markdown(f"### 📊 Chi tiết: {selected_bk_label}")
                if isinstance(bk_info, sqlite3.Row):
                    st.markdown("##### 💰 Tổng quan tài chính")
                    fin1, fin2, fin3 = st.columns(3)
                    net_p = bk_info['net_price'] or 0 # type: ignore
                    sell_p = bk_info['selling_price'] or 0 # type: ignore
                    prof_p = bk_info['profit'] or 0 # type: ignore
                    fin1.metric("Giá nét (đã gồm thuế)", format_vnd(net_p))
                    fin2.metric("Giá bán", format_vnd(sell_p))
                    fin3.metric("Lợi nhuận", format_vnd(prof_p))

                    if bk_info['customer_info']:
                        st.markdown(f"**👤 Khách hàng:** {bk_info['customer_info']}")
                    if bk_info['details']:
                        st.info(f"ℹ️ **Thông tin:** {bk_info['details']}")
                
                # Gọi hàm hiển thị so sánh
                render_cost_comparison(code)
                
                st.divider()
                # Nút hoàn tất & xóa booking
                c_complete, c_delete = st.columns(2)
                if c_complete.button("✅ Hoàn tất Booking", type="primary", use_container_width=True):
                    run_query("UPDATE service_bookings SET status='completed' WHERE code=?", (code,), commit=True)
                    st.success("Đã hoàn tất! Booking đã được chuyển sang tab Lịch sử."); time.sleep(1); st.rerun()

                if c_delete.button("🗑️ Xóa Booking này", use_container_width=True):
                    run_query("UPDATE service_bookings SET status='deleted' WHERE code=?", (code,), commit=True)
                    st.success("Đã xóa!"); time.sleep(0.5); st.rerun()
        else:
            st.info("Chưa có booking nào.")

    # ---------------- TAB 3: LỊCH SỬ BOOKING ----------------
    with tab3:
        st.subheader("📜 Lịch sử Booking đã hoàn tất")
        # --- Lọc danh sách booking theo sale ---
        hist_bk_query = "SELECT * FROM service_bookings WHERE status='completed'"
        hist_bk_params = []
        if current_user_role == 'sale' and current_user_name:
            hist_bk_query += " AND sale_name=?"
            hist_bk_params.append(current_user_name)
        hist_bk_query += " ORDER BY id DESC"
        history_bk = run_query(hist_bk_query, tuple(hist_bk_params))
        if history_bk:
            df_hist = pd.DataFrame([dict(r) for r in history_bk])
            st.dataframe(
                df_hist[['code', 'name', 'created_at', 'type', 'customer_info', 'details', 'net_price', 'selling_price', 'profit']],
                column_config={
                    "code": "Mã Booking",
                    "name": "Tên Booking",
                    "created_at": "Ngày tạo",
                    "type": "Loại",
                    "customer_info": "Khách hàng",
                    "details": "Chi tiết",
                    "net_price": st.column_config.NumberColumn("Giá nét", format="%d"),
                    "selling_price": st.column_config.NumberColumn("Giá bán", format="%d"),
                    "profit": st.column_config.NumberColumn("Lợi nhuận", format="%d"),
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Chưa có booking nào hoàn tất.")

def render_tour_management():
    st.title("📦 Quản Lý Tour ")
    
    # Sử dụng Tabs theo yêu cầu
    tab_est, tab_act, tab_hist, tab_rpt = st.tabs(["📝 Dự Toán Chi Phí", "💸 Quyết Toán Tour", "📜 Lịch sử Tour", "📈 Tổng Hợp Lợi Nhuận"])
    
    # Lấy thông tin user hiện tại để lọc
    current_user_info_tour = st.session_state.get("user_info", {})
    current_user_name_tour = current_user_info_tour.get('name', 'N/A')
    current_user_role_tour = current_user_info_tour.get('role')

    # Lấy danh sách Tour cho Selectbox dùng chung
    all_tours_query = "SELECT * FROM tours ORDER BY id DESC"
    all_tours_params = []
    if current_user_role_tour == 'sale' and current_user_name_tour:
        all_tours_query = "SELECT * FROM tours WHERE sale_name=? ORDER BY id DESC"
        all_tours_params.append(current_user_name_tour)
    all_tours = run_query(all_tours_query, tuple(all_tours_params))
    running_tours = [t for t in all_tours if t['status'] == 'running']
    tour_options = {f"[{t['tour_code']}] {t['tour_name']} ({t['start_date']})": t['id'] for t in running_tours} if running_tours else {} # type: ignore
    
    # ---------------- TAB 1: DỰ TOÁN CHI PHÍ ----------------
    with tab_est:
        with st.expander("➕ Tạo Thông Tin Đoàn Mới", expanded=False):
            # --- GỢI Ý KHÁCH HÀNG ---
            cust_query_t = "SELECT * FROM customers ORDER BY id DESC"
            cust_params_t = []
            if current_user_role_tour == 'sale' and current_user_name_tour:
                cust_query_t = "SELECT * FROM customers WHERE sale_name=? ORDER BY id DESC"
                cust_params_t.append(current_user_name_tour)
            customers = run_query(cust_query_t, tuple(cust_params_t))
            cust_opts_t = ["-- Khách mới --"] + [f"{c['name']} | {c['phone']}" for c in customers] if customers else ["-- Khách mới --"] # type: ignore
            sel_cust_t = st.selectbox("🔍 Gợi ý khách hàng:", cust_opts_t, key="tour_cust_suggest")
            
            t_pre_name, t_pre_phone = "", ""
            if sel_cust_t and sel_cust_t != "-- Khách mới --":
                parts = sel_cust_t.split(" | ")
                t_pre_name = parts[0]
                t_pre_phone = parts[1] if len(parts) > 1 else ""

            with st.form("create_tour_form", clear_on_submit=True):
                c1, c2 = st.columns(2)
                t_name = c1.text_input("Tên Đoàn")
                t_sale = c2.text_input("Sales phụ trách", value=current_user_name_tour, disabled=True)
                c_cust1, c_cust2 = st.columns(2)
                t_cust_name = c_cust1.text_input("Tên Khách / Đại diện", value=t_pre_name)
                t_cust_phone = c_cust2.text_input("SĐT Khách", value=t_pre_phone)
                c3, c4, c5 = st.columns(3)
                t_start = c3.date_input("Ngày đi", format="DD/MM/YYYY")
                t_end = c4.date_input("Ngày về", format="DD/MM/YYYY")
                t_pax = c5.number_input("Số lượng khách", min_value=1, step=1)
                
                if st.form_submit_button("Tạo Đoàn"):
                    if t_name:
                        save_customer_check(t_cust_name, t_cust_phone, current_user_name_tour)
                        new_tour_code = ''.join(random.choices(string.ascii_uppercase, k=5))
                        add_row_to_table('tours', {
                            'tour_name': t_name, 'sale_name': current_user_name_tour, 'start_date': t_start.strftime('%d/%m/%Y'),
                            'end_date': t_end.strftime('%d/%m/%Y'), 'guest_count': t_pax, 'created_at': datetime.now().strftime('%Y-%m-%d'),
                            'tour_code': new_tour_code, 'customer_name': t_cust_name, 'customer_phone': t_cust_phone
                        })
                        st.success(f"Đã tạo đoàn mới! Mã tour: {new_tour_code}. Hãy chọn ở danh sách bên dưới để làm dự toán.")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Vui lòng nhập tên đoàn.")

        st.divider()
        st.subheader("Bảng Tính Dự Toán")
        
        selected_tour_label = st.selectbox("Chọn Đoàn để làm dự toán:", list(tour_options.keys()) if tour_options else [], key="sel_tour_est")
        
        if selected_tour_label:
            tour_id = tour_options[selected_tour_label] # type: ignore
            tour_info = next((t for t in all_tours if t['id'] == tour_id), None)
            if not tour_info:
                st.error("Không tìm thấy thông tin tour.")
                st.stop()
            assert tour_info is not None

            # --- TOOLBAR: SỬA / XÓA TOUR ---
            c_ren, c_del = st.columns(2)
            with c_ren:
                with st.popover("✏️ Sửa thông tin", use_container_width=True):
                    with st.form(f"edit_tour_{tour_id}"):
                        en_n = st.text_input("Tên Đoàn", value=tour_info['tour_name']) # type: ignore
                        en_s = st.text_input("Sales", value=tour_info['sale_name']) # type: ignore
                        en_p = st.number_input("Số khách", value=tour_info['guest_count'], min_value=1) # type: ignore
                        if st.form_submit_button("Lưu thay đổi"):
                            if en_n != tour_info['tour_name']: # type: ignore
                                run_query("UPDATE tours SET pending_name=?, sale_name=?, guest_count=? WHERE id=?", (en_n, en_s, en_p, tour_id), commit=True)
                                st.success("Đã cập nhật thông tin & Gửi yêu cầu đổi tên (Chờ Admin duyệt)!"); time.sleep(0.5); st.rerun()
                            else:
                                run_query("UPDATE tours SET sale_name=?, guest_count=? WHERE id=?", (en_s, en_p, tour_id), commit=True)
                                st.success("Đã cập nhật!"); time.sleep(0.5); st.rerun()
            with c_del:
                req_status = tour_info['request_delete'] # type: ignore
                if req_status == 0:
                    with st.popover("🗑️ Yêu cầu xóa", use_container_width=True):
                        st.warning(f"Gửi yêu cầu xóa đoàn: {tour_info['tour_name']}?") # type: ignore
                        if st.button("Gửi yêu cầu", type="primary", use_container_width=True, key=f"req_del_t_{tour_id}"):
                            run_query("UPDATE tours SET request_delete=1 WHERE id=?", (tour_id,), commit=True)
                            st.success("Đã gửi yêu cầu xóa (Chờ Admin duyệt)!"); time.sleep(0.5); st.rerun()
                elif req_status == 1:
                    st.warning("⏳ Đang chờ Admin duyệt xóa...")
                    if st.button("Hủy yêu cầu", key=f"cancel_req_{tour_id}", use_container_width=True): # type: ignore
                        run_query("UPDATE tours SET request_delete=0 WHERE id=?", (tour_id,), commit=True)
                        st.rerun()
                elif req_status == 2:
                    st.success("✅ Admin đã duyệt xóa!")
                    c_conf, c_can = st.columns(2)
                    if c_conf.button("🗑️ Xóa ngay", type="primary", key=f"final_del_{tour_id}"): # type: ignore
                        run_query("DELETE FROM tours WHERE id=?", (tour_id,), commit=True)
                        run_query("DELETE FROM tour_items WHERE tour_id=?", (tour_id,), commit=True)
                        st.success("Đã xóa vĩnh viễn!"); time.sleep(0.5); st.rerun()
                    if c_can.button("Hủy xóa", key=f"keep_tour_{tour_id}"):
                        run_query("UPDATE tours SET request_delete=0 WHERE id=?", (tour_id,), commit=True)
                        st.rerun()

            # Reset edit mode when changing tour
            if st.session_state.current_tour_id_est != tour_id:
                st.session_state.est_edit_mode = False
                st.session_state.current_tour_id_est = tour_id
                if "est_df_temp" in st.session_state: del st.session_state.est_df_temp
                st.session_state.est_editor_key += 1
            
            # --- IMPORT EXCEL (MỚI - DỰ TOÁN) ---
            with st.expander("📥 Nhập dữ liệu từ Excel (Import)", expanded=False):
                st.caption("💡 File Excel cần có dòng tiêu đề: **Hạng mục, Diễn giải, Đơn vị, Đơn giá, Số lượng, Số lần**")
                
                # Widget upload file
                uploaded_est_file = st.file_uploader("Chọn file Excel dự toán", type=["xlsx", "xls"], key="up_est_tool")
                
                if uploaded_est_file:
                    if st.button("🚀 Đọc file & Điền vào bảng", type="primary"):
                        try:
                            # 1. Đọc file Excel (Tìm dòng tiêu đề tự động)
                            uploaded_est_file.seek(0)
                            df_raw = pd.read_excel(uploaded_est_file, header=None)
                            
                            header_idx = 0
                            detect_kws = ['hạng mục', 'tên hàng', 'diễn giải', 'đơn giá', 'số lượng', 'thành tiền', 'item', 'price', 'qty', 'đvt']
                            
                            # Quét 15 dòng đầu để tìm dòng chứa nhiều từ khóa nhất
                            for i in range(min(15, len(df_raw))):
                                row_vals = [str(x).lower() for x in df_raw.iloc[i].tolist()]
                                if sum(1 for kw in detect_kws if any(kw in val for val in row_vals)) >= 2:
                                    header_idx = i
                                    break
                            
                            uploaded_est_file.seek(0)
                            df_in = pd.read_excel(uploaded_est_file, header=header_idx)
                            
                            # 2. Chuẩn hóa tên cột
                            # Chuyển hết về chữ thường để so sánh
                            df_in.columns = [str(c).lower().strip() for c in df_in.columns]
                            
                            # Định nghĩa các từ khóa (Aliases) cho từng cột DB - Ưu tiên từ trái sang phải
                            col_aliases = {
                                'category': ['hạng mục', 'hang muc', 'tên hàng', 'ten hang', 'tên dịch vụ', 'ten dich vu', 'nội dung', 'noi dung', 'item'],
                                'description': ['diễn giải', 'dien giai', 'chi tiết', 'chi tiet', 'ghi chú', 'ghi chu', 'mô tả', 'mo ta', 'description', 'desc'],
                                'unit': ['đơn vị', 'don vi', 'đvt', 'dvt', 'unit', 'uom'],
                                'quantity': ['số lượng', 'so luong', 'sl', 'qty', 'quantity', 'vol'],
                                'unit_price': ['đơn giá', 'don gia', 'giá', 'gia', 'price', 'unit_price', 'unit price'],
                                'times': ['số lần', 'so lan', 'lần', 'lan', 'times']
                            }
                            
                            # Xác định cột nào trong Excel map vào cột nào trong DB
                            final_col_map = {}
                            for db_col, aliases in col_aliases.items():
                                for alias in aliases:
                                    if alias in df_in.columns:
                                        final_col_map[db_col] = alias
                                        break
                            
                            new_data = []
                            if not final_col_map:
                                st.warning("⚠️ Không tìm thấy các cột thông tin cần thiết (Hạng mục, Đơn giá...). Vui lòng kiểm tra tên cột trong file Excel.")
                            else:
                                for _, row in df_in.iterrows():
                                    item = {}
                                    for db_col, xls_col in final_col_map.items():
                                        val = row[xls_col]
                                        if pd.isna(val):
                                            val = 0 if db_col in ['quantity', 'unit_price', 'times'] else ""
                                        item[db_col] = val
                                    
                                    # Default values
                                    if 'category' not in item: item['category'] = ""
                                    if 'description' not in item: item['description'] = ""
                                    if 'unit' not in item: item['unit'] = ""
                                    
                                    # Safe numeric conversion
                                    def safe_float(v):
                                        try: return float(v)
                                        except: return 0.0
                                    
                                    item['quantity'] = safe_float(item.get('quantity', 1))
                                    item['unit_price'] = safe_float(item.get('unit_price', 0))
                                    item['times'] = safe_float(item.get('times', 1))
                                    if item['times'] == 0: item['times'] = 1
                                    
                                    if str(item['category']).strip() or str(item['description']).strip():
                                        new_data.append(item)

                            if new_data:
                                # 3. Cập nhật vào Session State (Hiển thị lên màn hình)
                                st.session_state.est_df_temp = pd.DataFrame(new_data)
                                st.session_state.est_edit_mode = True # Bật chế độ sửa để hiện nút Lưu
                                st.success(f"Đã đọc thành công {len(new_data)} dòng! Vui lòng kiểm tra bảng bên dưới và bấm LƯU.")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.warning(f"Không đọc được dữ liệu! (Đã thử dòng {header_idx+1} làm tiêu đề). Vui lòng kiểm tra tên cột.")
                                
                        except Exception as e:
                            st.error(f"Lỗi khi đọc file: {str(e)}")

            # --- Fetch Items (EST) ---
            if "est_df_temp" not in st.session_state:
                existing_items = run_query("SELECT * FROM tour_items WHERE tour_id=? AND item_type='EST'", (tour_id,))
                if existing_items:
                    df_est = pd.DataFrame([dict(r) for r in existing_items])
                    if 'times' not in df_est.columns: df_est['times'] = 1.0
                    df_est = df_est[['category', 'description', 'unit', 'unit_price', 'quantity', 'times']]
                else:
                    df_est = pd.DataFrame([
                        {"category": "Vận chuyển", "description": "Xe 16 chỗ", "unit": "Xe", "unit_price": 0, "quantity": 1, "times": 1},
                        {"category": "Lưu trú", "description": "Khách sạn 3 sao", "unit": "Phòng", "unit_price": 0, "quantity": 1, "times": 1},
                        {"category": "Ăn uống", "description": "Bữa trưa ngày 1", "unit": "Suất", "unit_price": 0, "quantity": 1, "times": 1},
                    ])
                st.session_state.est_df_temp = df_est

            # Prepare Display Data (Tạo bản sao để hiển thị format đẹp)
            df_display = st.session_state.est_df_temp.copy()
            
            # [MODIFIED] Tính Giá/Pax và ẩn cột Times
            guest_cnt = tour_info['guest_count'] if tour_info['guest_count'] else 1 # type: ignore
            df_display['total_val'] = df_display['quantity'] * df_display['unit_price'] * df_display['times']
            df_display['price_per_pax'] = df_display['total_val'] / guest_cnt
            
            df_display['price_per_pax'] = df_display['price_per_pax'].apply(lambda x: format_vnd(x) + " VND")
            df_display['total_display'] = df_display['total_val'].apply(lambda x: format_vnd(x) + " VND")
            df_display['unit_price'] = df_display['unit_price'].apply(lambda x: format_vnd(x) + " VND") # type: ignore

            st.markdown(f"**Đoàn:** {tour_info['tour_name']} (Mã: {tour_info['tour_code']}) | **Pax:** {tour_info['guest_count']}")
            
            is_disabled = not st.session_state.est_edit_mode

            # --- DATA EDITOR ---
            edited_est = st.data_editor(
                df_display,
                disabled=is_disabled,
                num_rows="dynamic",
                column_config={
                    "category": st.column_config.TextColumn("Hạng mục chi phí", required=False),
                    "description": st.column_config.TextColumn("Diễn giải"),
                    "unit": st.column_config.TextColumn("Đơn vị"),
                    "unit_price": st.column_config.TextColumn("Đơn giá (VND)", required=False),
                    "quantity": st.column_config.NumberColumn("Số lượng", min_value=0),
                    "times": st.column_config.NumberColumn("Số lần", min_value=1),
                    "price_per_pax": st.column_config.TextColumn("Giá/Pax", disabled=True),
                    "total_display": st.column_config.TextColumn("Tổng chi phí", disabled=True),
                    "total_val": st.column_config.NumberColumn("Hidden", disabled=True),
                },
                column_order=("category", "description", "unit", "unit_price", "quantity", "times", "price_per_pax", "total_display"),
                use_container_width=True,
                hide_index=True,
                key=f"editor_est_{st.session_state.est_editor_key}"
            )
            
            # --- AUTO-UPDATE CALCULATION ---
            if st.session_state.est_edit_mode:
                # Tự động cập nhật khi dữ liệu thay đổi
                df_new = edited_est.copy()
                
                def clean_vnd_auto(x):
                    if isinstance(x, str):
                        return float(x.replace('.', '').replace(',', '').replace(' VND', '').strip())
                    return float(x) if x else 0.0
                
                df_new['unit_price'] = df_new['unit_price'].apply(clean_vnd_auto)
                df_new['quantity'] = pd.to_numeric(df_new['quantity'], errors='coerce').fillna(0)
                if 'times' not in df_new.columns: df_new['times'] = 1
                df_new['times'] = pd.to_numeric(df_new['times'], errors='coerce').fillna(1)
                
                # So sánh với dữ liệu cũ
                cols_check = ['category', 'description', 'unit', 'unit_price', 'quantity', 'times']
                df_old = st.session_state.est_df_temp.copy()
                if 'times' not in df_old.columns: df_old['times'] = 1
                
                # Reset index và fillna để so sánh
                df_new_check = df_new[cols_check].reset_index(drop=True).fillna(0)
                df_old_check = df_old[cols_check].reset_index(drop=True).fillna(0)
                
                has_changes = False
                if len(df_new_check) != len(df_old_check): has_changes = True
                elif not df_new_check.equals(df_old_check): has_changes = True
                
                if has_changes:
                    st.session_state.est_df_temp = df_new[cols_check]
                    st.rerun()

            # --- TÍNH TOÁN REAL-TIME ---
            total_cost = 0
            if not edited_est.empty:
                # [FIX] Handle case where a cell is None, which becomes the string 'None' after astype(str)
                cleaned_prices_est = edited_est['unit_price'].astype(str).str.replace('.', '', regex=False).str.replace(' VND', '', regex=False).str.strip()
                p_price = cleaned_prices_est.apply(lambda x: float(x) if x and x.lower() != 'none' else 0.0)
                t_times = edited_est['times'].fillna(1) # type: ignore
                total_cost = (edited_est['quantity'] * p_price * t_times).sum()
            
            st.divider()
            
            # --- PHẦN TÍNH LỢI NHUẬN & THUẾ (YÊU CẦU 2: Sắp xếp hàng ngang) ---
            c_cost, c_profit, c_tax = st.columns(3)
            
            with c_cost:
                st.metric("Tổng Chi Phí Dự Toán", format_vnd(total_cost) + " VND") # type: ignore
            with c_profit:
                p_percent = st.number_input("Lợi Nhuận Mong Muốn (%)", value=float(tour_info['est_profit_percent']), step=0.5, key="p_pct", disabled=is_disabled) # type: ignore
            with c_tax:
                t_percent = st.number_input("Thuế VAT Đầu Ra (%)", value=float(tour_info['est_tax_percent']), step=1.0, key="t_pct", disabled=is_disabled) # type: ignore
            
            # Công thức: Giá Bán = Chi Phí + Lợi Nhuận + Thuế
            # Lợi nhuận = Chi Phí * %
            # Thuế = (Chi Phí + Lợi Nhuận) * %
            profit_amt = total_cost * (p_percent / 100)
            base_price = total_cost + profit_amt
            tax_amt = base_price * (t_percent / 100)
            final_price = base_price + tax_amt

            st.markdown(f"""<div class="finance-summary-card">
                <div class="row"><span>Tiền Lợi Nhuận ({p_percent}%):</span> <b>{format_vnd(profit_amt)} VND</b></div>
                <div class="row"><span>Tiền Thuế ({t_percent}%):</span> <b>{format_vnd(tax_amt)} VND</b></div>
                <div class="row total-row"><span>TỔNG GIÁ BÁN DỰ KIẾN:</span> <b>{format_vnd(final_price)} VND</b></div>
                <div class="pax-price">(Giá trung bình/khách: {format_vnd(final_price/tour_info['guest_count'] if tour_info['guest_count'] else 1)} VND)</div>
            </div>
            """, unsafe_allow_html=True)

            # --- THÊM Ô NHẬP GIÁ CHỐT & GIÁ TRẺ EM ---
            st.write("")
            t_dict: Dict[str, Any] = dict(tour_info) if tour_info else {}
            c_final_p, c_child_p = st.columns(2)
            with c_final_p:
                # Giá chốt tour - Text Input for dots formatting
                cur_final_price = float(t_dict.get('final_tour_price', 0) or 0)
                cur_final_price_str = "{:,.0f}".format(cur_final_price).replace(",", ".")
                final_tour_price_input = st.text_input("Giá chốt tour (VND)", value=cur_final_price_str, disabled=is_disabled, help="Nhập số tiền (VD: 1.000.000)")
                try: final_tour_price_val = float(final_tour_price_input.replace('.', '').replace(',', ''))
                except: final_tour_price_val = 0.0

                # Số lượng người lớn
                cur_qty = float(t_dict.get('final_qty', 0))
                if cur_qty == 0: cur_qty = float(t_dict.get('guest_count', 1))
                final_qty_val = st.number_input("Số lượng người lớn", value=cur_qty, min_value=0.0, step=1.0, disabled=is_disabled)

            with c_child_p:
                # Giá trẻ em - Text Input
                cur_child_price = float(t_dict.get('child_price', 0) or 0)
                cur_child_price_str = "{:,.0f}".format(cur_child_price).replace(",", ".")
                child_price_input = st.text_input("Giá trẻ em (VND)", value=cur_child_price_str, disabled=is_disabled)
                try: child_price_val = float(child_price_input.replace('.', '').replace(',', ''))
                except: child_price_val = 0.0

                cur_child_qty = float(t_dict.get('child_qty', 0))
                child_qty_val = st.number_input("Số lượng trẻ em", value=cur_child_qty, min_value=0.0, step=1.0, disabled=is_disabled)
            
            total_final_manual = (final_tour_price_val * final_qty_val) + (child_price_val * child_qty_val)
            st.markdown(f"""<div style="background-color: #e8f5e9; padding: 15px; border-radius: 10px; margin-top: 10px; border: 1px solid #c8e6c9;"><div style="display:flex; justify-content:space-between; font-size: 1.3em; color: #2e7d32;"><span><b>TỔNG DOANH THU</b></span> <b>{format_vnd(total_final_manual)} VND</b></div></div>""", unsafe_allow_html=True)

            # --- EXPORT EXCEL ---
            st.write("")
            df_exp = st.session_state.est_df_temp.copy()
            
            # Chuẩn hóa dữ liệu số
            def clean_price_exp(x): # type: ignore
                if isinstance(x, str):
                    return float(x.replace('.', '').replace(',', '').replace(' VND', '').strip())
                return float(x) if x else 0.0
            
            df_exp['unit_price'] = df_exp['unit_price'].apply(clean_price_exp)
            df_exp['quantity'] = pd.to_numeric(df_exp['quantity'], errors='coerce').fillna(0)
            if 'times' not in df_exp.columns: df_exp['times'] = 1
            df_exp['times'] = pd.to_numeric(df_exp['times'], errors='coerce').fillna(1)
            
            # Tính toán các cột hiển thị giống Web
            df_exp['total_amount'] = df_exp['quantity'] * df_exp['unit_price'] * df_exp['times']
            g_cnt = tour_info['guest_count'] if tour_info['guest_count'] else 1 # type: ignore
            df_exp['price_per_pax'] = df_exp['total_amount'] / g_cnt
            
            # Chọn và đổi tên cột
            df_exp = df_exp[['category', 'description', 'unit', 'unit_price', 'quantity', 'times', 'price_per_pax', 'total_amount']]
            df_exp.columns = ['Hạng mục', 'Diễn giải', 'Đơn vị', 'Đơn giá', 'Số lượng', 'Số lần', 'Giá/Pax', 'Tổng chi phí']

            buffer = io.BytesIO()
            file_ext = "xlsx"
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            try:
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer: # type: ignore
                        # Start table at row 11 (index 10) to leave space for info
                        start_row = 10
                        df_exp.to_excel(writer, index=False, sheet_name='DuToan', startrow=start_row)
                        
                        # --- FORMATTING (Nếu dùng xlsxwriter) ---
                        workbook: Any = writer.book
                        worksheet = writer.sheets['DuToan']
                        
                        # --- STYLES ---
                        company_name_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#1B5E20'})
                        company_info_fmt = workbook.add_format({'font_size': 10, 'italic': True, 'font_color': '#424242'})
                        
                        title_fmt = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter', 'font_color': '#0D47A1', 'bg_color': '#E3F2FD', 'border': 1})
                        section_fmt = workbook.add_format({'bold': True, 'font_size': 11, 'font_color': '#E65100', 'underline': True})
                        
                        header_fmt = workbook.add_format({'bold': True, 'fg_color': '#2E7D32', 'font_color': 'white', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                        body_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'text_wrap': True, 'font_size': 10})
                        body_center_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'align': 'center', 'font_size': 10})
                        money_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'num_format': '#,##0', 'font_size': 10})
                        
                        # Summary Section Styles
                        sum_header_bg_fmt = workbook.add_format({'bold': True, 'bg_color': '#FFF3E0', 'border': 1, 'font_color': '#E65100', 'align': 'center', 'valign': 'vcenter'})
                        sum_label_fmt = workbook.add_format({'bold': True, 'align': 'left', 'border': 1, 'bg_color': '#FAFAFA'})
                        sum_val_fmt = workbook.add_format({'num_format': '#,##0', 'align': 'right', 'border': 1})
                        sum_val_bold_fmt = workbook.add_format({'bold': True, 'num_format': '#,##0', 'align': 'right', 'border': 1})
                        sum_total_fmt = workbook.add_format({'bold': True, 'bg_color': '#C8E6C9', 'font_color': '#1B5E20', 'num_format': '#,##0', 'align': 'right', 'border': 1, 'font_size': 12})
                        
                        # --- 1. COMPANY INFO (Rows 0-3) ---
                        if comp['logo_b64_str']:
                            try:
                                logo_data = base64.b64decode(comp['logo_b64_str'])
                                image_stream = io.BytesIO(logo_data)
                                img = Image.open(image_stream)
                                w, h = img.size
                                scale = 60 / h if h > 0 else 0.5
                                image_stream.seek(0)
                                worksheet.insert_image('A1', 'logo.png', {'image_data': image_stream, 'x_scale': scale, 'y_scale': scale, 'x_offset': 5, 'y_offset': 5})
                            except: pass
                        
                        worksheet.write('B1', comp['name'], company_name_fmt)
                        worksheet.write('B2', f"ĐC: {comp['address']}", company_info_fmt)
                        worksheet.write('B3', f"SĐT: {comp['phone']}", company_info_fmt)
                        
                        # --- 2. TOUR INFO (Rows 4-9) ---
                        worksheet.merge_range('A5:G5', "BẢNG DỰ TOÁN CHI PHÍ TOUR", title_fmt)
                        
                        # Info Data
                        t_info_dict = dict(tour_info) if tour_info else {}
                        t_name = t_info_dict.get('tour_name', '')
                        t_code = t_info_dict.get('tour_code', '')
                        t_sale = t_info_dict.get('sale_name', '')
                        t_start = t_info_dict.get('start_date', '')
                        t_end = t_info_dict.get('end_date', '')
                        t_cust = t_info_dict.get('customer_name', '')
                        t_phone = t_info_dict.get('customer_phone', '')
                        t_guest = t_info_dict.get('guest_count', 0)
                        
                        # Layout Info nicely
                        worksheet.write('A7', "Tên đoàn:", sum_label_fmt)
                        worksheet.merge_range('B7:D7', t_name, sum_val_fmt)
                        worksheet.write('E7', "Mã đoàn:", sum_label_fmt)
                        worksheet.merge_range('F7:G7', t_code, sum_val_fmt)
                        
                        worksheet.write('A8', "Khách hàng:", sum_label_fmt)
                        worksheet.merge_range('B8:D8', f"{t_cust} - {t_phone}", sum_val_fmt)
                        worksheet.write('E8', "Sales:", sum_label_fmt)
                        worksheet.merge_range('F8:G8', t_sale, sum_val_fmt)
                        
                        worksheet.write('A9', "Thời gian:", sum_label_fmt)
                        worksheet.merge_range('B9:D9', f"{t_start} - {t_end}", sum_val_fmt)
                        worksheet.write('E9', "Số khách:", sum_label_fmt)
                        worksheet.merge_range('F9:G9', t_guest, sum_val_fmt)

                        # --- 3. TABLE HEADER & BODY ---
                        # Apply Header
                        for col_num, value in enumerate(df_exp.columns):
                            worksheet.write(start_row, col_num, value, header_fmt)
                        
                        # Apply Body
                        for row in range(len(df_exp)):
                            for col in range(len(df_exp.columns)):
                                val = df_exp.iloc[row, col]
                                # Cols: 0=Cat, 1=Desc, 2=Unit, 3=Price, 4=Qty, 5=PaxPrice, 6=Total
                                if col == 2: fmt = body_center_fmt # Unit centered
                                elif col in [3, 4, 5, 6, 7]: fmt = money_fmt # Money columns
                                else: fmt = body_fmt
                                
                                if pd.isna(val): val = ""
                                worksheet.write(row+start_row+1, col, val, fmt)
                        
                        # --- 4. SUMMARY SECTION ---
                        last_row = start_row + 1 + len(df_exp)
                        sum_row = last_row + 2
                        
                        # --- BẢNG TÍNH GIÁ THÀNH ---
                        worksheet.merge_range(sum_row, 0, sum_row, 3, "PHÂN TÍCH GIÁ THÀNH & LỢI NHUẬN", sum_header_bg_fmt)
                        
                        # Dòng 1: Tổng chi phí
                        worksheet.write(sum_row+1, 0, "1. Tổng chi phí dự toán:", sum_label_fmt)
                        worksheet.merge_range(sum_row+1, 1, sum_row+1, 3, total_cost, sum_val_bold_fmt)
                        
                        # Dòng 2: Lợi nhuận
                        worksheet.write(sum_row+2, 0, "2. Lợi nhuận mong muốn:", sum_label_fmt)
                        worksheet.write(sum_row+2, 1, f"{p_percent:g}%", body_center_fmt)
                        worksheet.merge_range(sum_row+2, 2, sum_row+2, 3, profit_amt, sum_val_fmt)
                        
                        # Dòng 3: Thuế
                        worksheet.write(sum_row+3, 0, "3. Thuế VAT:", sum_label_fmt)
                        worksheet.write(sum_row+3, 1, f"{t_percent:g}%", body_center_fmt)
                        worksheet.merge_range(sum_row+3, 2, sum_row+3, 3, tax_amt, sum_val_fmt)
                        
                        # Dòng 4: Giá tính toán
                        worksheet.write(sum_row+4, 0, "4. Giá bán tính toán:", sum_label_fmt)
                        worksheet.merge_range(sum_row+4, 1, sum_row+4, 3, final_price, sum_total_fmt)
                        
                        # --- BẢNG CHỐT GIÁ BÁN ---
                        # Đặt bên phải bảng giá thành (Cột E, F, G)
                        worksheet.merge_range(sum_row, 4, sum_row, 6, "BẢNG CHỐT GIÁ BÁN THỰC TẾ", sum_header_bg_fmt)
                        
                        # Người lớn
                        worksheet.write(sum_row+1, 4, "Người lớn:", sum_label_fmt)
                        worksheet.write(sum_row+1, 5, final_qty_val, sum_val_fmt) # SL
                        worksheet.write(sum_row+1, 6, final_tour_price_val, sum_val_fmt) # Giá
                        
                        # Trẻ em
                        worksheet.write(sum_row+2, 4, "Trẻ em:", sum_label_fmt)
                        worksheet.write(sum_row+2, 5, child_qty_val, sum_val_fmt) # SL
                        worksheet.write(sum_row+2, 6, child_price_val, sum_val_fmt) # Giá
                        
                        # Tổng doanh thu
                        worksheet.write(sum_row+4, 4, "TỔNG DOANH THU:", sum_label_fmt)
                        worksheet.merge_range(sum_row+4, 5, sum_row+4, 6, total_final_manual, sum_total_fmt)

                        # Column Widths
                        worksheet.set_column('A:A', 25) # Category
                        worksheet.set_column('B:B', 40) # Desc
                        worksheet.set_column('C:C', 10) # Unit
                        worksheet.set_column('D:G', 18) # Numbers
            except Exception as e:
                # If xlsxwriter fails, fall back to a simple CSV export
                buffer.seek(0)
                buffer.truncate()
                df_exp.to_csv(buffer, index=False, encoding='utf-8-sig')
                file_ext = "csv"
                mime_type = "text/csv"
                st.error(f"⚠️ Lỗi khi tạo file Excel: {e}. Đã chuyển sang xuất file CSV.")
                st.info("💡 Gợi ý: Nếu bạn vừa cài thư viện, hãy TẮT HẲN ứng dụng (Ctrl+C tại terminal) và chạy lại lệnh `streamlit run app.py`.")

            clean_t_name = re.sub(r'[\\/*?:"<>|]', "", tour_info['tour_name'] if tour_info else "Tour") # type: ignore
            st.download_button(label=f"📥 Tải Bảng Dự Toán ({file_ext.upper()})", data=buffer.getvalue(), file_name=f"DuToan_{clean_t_name}.{file_ext}", mime=mime_type, use_container_width=True)

            # --- Nút Chỉnh sửa / Lưu ---
            if st.session_state.est_edit_mode:
                if st.button("💾 LƯU DỰ TOÁN", type="primary", use_container_width=True):
                    # 1. Update Tour Meta
                    run_query("UPDATE tours SET est_profit_percent=?, est_tax_percent=?, final_tour_price=?, child_price=?, final_qty=?, child_qty=? WHERE id=?", (p_percent, t_percent, final_tour_price_val, child_price_val, final_qty_val, child_qty_val, tour_id), commit=True)
                    
                    # 2. Update Tour Items (Xóa cũ thêm mới)
                    run_query("DELETE FROM tour_items WHERE tour_id=? AND item_type='EST'", (tour_id,), commit=True)

                    data_to_insert = []
                    query = """INSERT INTO tour_items (tour_id, item_type, category, description, unit, quantity, unit_price, total_amount, times)
                               VALUES (?, 'EST', ?, ?, ?, ?, ?, ?, ?)"""

                    for _, row in edited_est.iterrows():
                        if row['category'] or row['description']: # type: ignore
                            # Xử lý dữ liệu
                            u_price = float(str(row['unit_price']).replace('.', '').replace(' VND', '').strip()) if row['unit_price'] else 0 # type: ignore
                            t_times = row.get('times', 1) # type: ignore
                            if pd.isna(t_times): t_times = 1
                            total_row = row['quantity'] * u_price * t_times # type: ignore
                            
                            # Thêm vào danh sách chờ (chưa ghi ngay)
                            data_to_insert.append((
                                tour_id, 
                                row['category'], 
                                row['description'], 
                                row['unit'], 
                                row['quantity'],  # type: ignore
                                u_price, 
                                total_row, 
                                t_times
                            ))

                    # Ghi tất cả trong 1 lần bắn
                    if data_to_insert:
                        run_query_many(query, data_to_insert)

                    if "est_df_temp" in st.session_state: del st.session_state.est_df_temp
                    st.session_state.est_edit_mode = False
                    st.success("Đã lưu dự toán thành công!")
                    time.sleep(1); st.rerun()
            else:
                if st.button("✏️ Chỉnh sửa Dự toán", use_container_width=True):
                    st.session_state.est_edit_mode = True
                    st.rerun()

    # ---------------- TAB 2: QUYẾT TOÁN ----------------
    with tab_act:
        st.subheader("💸 Quyết Toán ")
        
        selected_tour_act_label = st.selectbox("Chọn Đoàn quyết toán:", list(tour_options.keys()) if tour_options else [], key="sel_tour_act")
        
        if selected_tour_act_label:
            tour_id_act = tour_options[selected_tour_act_label] # type: ignore
            tour_info_act = next((t for t in all_tours if t['id'] == tour_id_act), None)
            if not tour_info_act:
                st.error("Không tìm thấy thông tin tour.")
                st.stop()
            assert tour_info_act is not None
            
            # --- Lấy Dự toán để so sánh ---
            est_items = run_query("SELECT SUM(total_amount) as total FROM tour_items WHERE tour_id=? AND item_type='EST'", (tour_id_act,), fetch_one=True)
            # If the query returns a row and the 'total' is not None (SQL SUM can return NULL), use it. Otherwise, default to 0.
            est_total_cost = est_items['total'] if est_items and est_items['total'] is not None else 0
            # Tính lại giá bán chốt (Dựa trên % đã lưu)
            p_pct = tour_info_act['est_profit_percent'] # type: ignore
            t_pct = tour_info_act['est_tax_percent'] # type: ignore
            est_profit_val = est_total_cost * (p_pct / 100)
            est_final_sale = (est_total_cost + est_profit_val) * (1 + t_pct/100)
            
            # [UPDATED] Lấy Tổng doanh thu từ bên Dự toán (Giá chốt * SL)
            t_act_dict_calc = dict(tour_info_act)
            final_price_est = float(t_act_dict_calc.get('final_tour_price', 0) or 0)
            child_price_est = float(t_act_dict_calc.get('child_price', 0) or 0)
            final_qty_est = float(t_act_dict_calc.get('final_qty', 0) or 0)
            child_qty_est = float(t_act_dict_calc.get('child_qty', 0) or 0)
            if final_qty_est == 0: final_qty_est = float(t_act_dict_calc.get('guest_count', 1))
            total_revenue_est = (final_price_est * final_qty_est) + (child_price_est * child_qty_est)
            
            if total_revenue_est > 0:
                est_final_sale = total_revenue_est
            else:
                est_profit_val = est_total_cost * (p_pct / 100)
                est_final_sale = (est_total_cost + est_profit_val) * (1 + t_pct/100)
            
            st.info(f"TỔNG DOANH THU: {format_vnd(est_final_sale)} VND")

            # --- [UPDATED] PHÂN TÍCH CHI PHÍ ---
            st.divider()
            st.markdown("### 📊 Phân tích Chi phí")
            
            linked_docs = run_query("SELECT * FROM invoices WHERE cost_code=? AND status='active'", (tour_info_act['tour_code'],)) # type: ignore
            df_linked = pd.DataFrame([dict(r) for r in linked_docs]) if linked_docs else pd.DataFrame()
            
            total_unc = 0
            total_inv = 0
            df_unc = pd.DataFrame()
            df_inv = pd.DataFrame()

            if not df_linked.empty:
                unc_mask = df_linked['invoice_number'].astype(str).str.contains("UNC", case=False, na=False) # type: ignore
                df_unc = df_linked.loc[unc_mask]
                total_unc = df_unc['total_amount'].sum()
                
                inv_mask = (df_linked['type'] == 'IN') & (~unc_mask)
                df_inv = df_linked.loc[inv_mask]
                total_inv = df_inv['total_amount'].sum()

            c_unc_t, c_inv_t = st.columns(2)
            with c_unc_t:
                st.markdown(f"#### 💸 1. Chi phí UNC: {format_vnd(total_unc)}")
                if not df_unc.empty:
                    # [UPDATED] Format tiền tệ Việt Nam có dấu chấm và chữ VND
                    df_unc_show = df_unc.copy()
                    df_unc_show['total_show'] = df_unc_show['total_amount'].apply(lambda x: format_vnd(x) + " VND") # type: ignore
                    st.dataframe(df_unc_show[['date', 'invoice_number', 'memo', 'total_show']],
                                 column_config={
                                     "date": "Ngày", 
                                     "invoice_number": "Số chứng từ", 
                                     "memo": "Nội dung", 
                                     "total_show": "Thành tiền"
                                 },
                                 use_container_width=True, hide_index=True)
                else: st.caption("Chưa có UNC.")
            
            with c_inv_t:
                st.markdown(f"#### 📄 2. Hóa đơn đầu vào: {format_vnd(total_inv)}")
                if not df_inv.empty:
                    # [UPDATED] Format tiền tệ Việt Nam có dấu chấm và chữ VND
                    df_inv_show = df_inv.copy()
                    df_inv_show['total_show'] = df_inv_show['total_amount'].apply(lambda x: format_vnd(x) + " VND") # type: ignore
                    st.dataframe(df_inv_show[['date', 'invoice_number', 'seller_name', 'total_show']], 
                                 column_config={"date": "Ngày", "invoice_number": "Số hóa đơn", "seller_name": "Đơn vị bán", "total_show": "Thành tiền"}, 
                                 use_container_width=True, hide_index=True)
                else: st.caption("Chưa có hóa đơn đầu vào.")

            # [CODE MỚI] Lấy dữ liệu Dự toán để so sánh
            est_items_ref = run_query("SELECT category, description, total_amount FROM tour_items WHERE tour_id=? AND item_type='EST'", (tour_id_act,))
            est_lookup = {}
            if est_items_ref:
                for r in est_items_ref:
                    key = (str(r['category']).strip().lower(), str(r['description']).strip().lower()) # type: ignore
                    est_lookup[key] = float(r['total_amount'] or 0) # type: ignore
            
            with st.expander("👀 Bảng Dự Toán (Để đối chiếu)", expanded=False):
                if est_items_ref:
                    df_est_ref = pd.DataFrame([dict(r) for r in est_items_ref])
                    df_est_ref['total_amount'] = df_est_ref['total_amount'].apply(lambda x: format_vnd(x)) # type: ignore
                    st.dataframe(df_est_ref, column_config={"category": "Hạng mục", "description": "Diễn giải", "total_amount": "Dự toán"}, use_container_width=True, hide_index=True)
                else: st.info("Chưa có dữ liệu dự toán.")

            # --- Fetch Items (ACT) with Session State ---
            if "current_tour_id_act" not in st.session_state: st.session_state.current_tour_id_act = None
            if st.session_state.current_tour_id_act != tour_id_act:
                if "act_df_temp" in st.session_state: del st.session_state.act_df_temp
                st.session_state.current_tour_id_act = tour_id_act

            if "act_df_temp" not in st.session_state:
                act_items = run_query("SELECT * FROM tour_items WHERE tour_id=? AND item_type='ACT'", (tour_id_act,))
                if act_items:
                    df_act = pd.DataFrame([dict(r) for r in act_items])
                    if 'times' not in df_act.columns: df_act['times'] = 1.0
                    df_act = df_act[['category', 'description', 'unit', 'unit_price', 'quantity', 'times']]
                else:
                     # Gợi ý: Nếu chưa có item ACT, load item EST để sửa cho nhanh
                     est_items_raw = run_query("SELECT * FROM tour_items WHERE tour_id=? AND item_type='EST'", (tour_id_act,))
                     if est_items_raw:
                         df_act = pd.DataFrame([dict(r) for r in est_items_raw])
                         if 'times' not in df_act.columns: df_act['times'] = 1.0
                         df_act = df_act[['category', 'description', 'unit', 'unit_price', 'quantity', 'times']]
                     else:
                         df_act = pd.DataFrame([{"category": "", "description": "", "unit": "", "quantity": 0, "unit_price": 0, "times": 1}])
                st.session_state.act_df_temp = df_act

            # Prepare Display Data
            df_act_display = st.session_state.act_df_temp.copy()
            guest_cnt_act = tour_info_act['guest_count'] if tour_info_act['guest_count'] else 1 # type: ignore
            
            # Calculate numeric totals
            # Ensure numeric types
            df_act_display['quantity'] = pd.to_numeric(df_act_display['quantity'], errors='coerce').fillna(0)
            df_act_display['unit_price'] = pd.to_numeric(df_act_display['unit_price'], errors='coerce').fillna(0)
            df_act_display['times'] = pd.to_numeric(df_act_display['times'], errors='coerce').fillna(1)

            # Formula: Total = Unit * Qty * Times
            df_act_display['total_val'] = df_act_display['quantity'] * df_act_display['unit_price'] * df_act_display['times']
            # Formula: Pax = Total / Guests
            df_act_display['price_per_pax'] = df_act_display['total_val'] / guest_cnt_act
            
            # Format strings
            df_act_display['price_per_pax'] = df_act_display['price_per_pax'].apply(lambda x: format_vnd(x) + " VND")
            df_act_display['total_display'] = df_act_display['total_val'].apply(lambda x: format_vnd(x) + " VND") # type: ignore
            df_act_display['unit_price'] = df_act_display['unit_price'].apply(lambda x: format_vnd(x) + " VND") # type: ignore

            # [CODE MỚI] Tính toán so sánh (Dự toán vs Thực tế)
            def get_est_val(row): # type: ignore
                k = (str(row['category']).strip().lower(), str(row['description']).strip().lower()) # type: ignore
                return est_lookup.get(k, 0.0)
            
            df_act_display['est_val'] = df_act_display.apply(get_est_val, axis=1)
            df_act_display['diff_val'] = df_act_display['est_val'] - df_act_display['total_val']
            df_act_display['est_display'] = df_act_display['est_val'].apply(lambda x: format_vnd(x) + " VND")
            df_act_display['diff_display'] = df_act_display['diff_val'].apply(lambda x: format_vnd(x) + " VND")

            # --- LOGIC KHÓA / DUYỆT QUYẾT TOÁN ---
            req_act_status = tour_info_act['request_edit_act'] # type: ignore
            has_act_data = False
            check_act = run_query("SELECT id FROM tour_items WHERE tour_id=? AND item_type='ACT' LIMIT 1", (tour_id_act,))
            if check_act: has_act_data = True

            is_act_editable = False
            if current_user_role_tour in ['admin', 'admin_f1']:
                is_act_editable = True

            st.divider()
            st.markdown("#### ✍️ 3.Quyết toán")
            edited_act = st.data_editor(
                df_act_display,
                num_rows="dynamic",
                column_config={
                    "category": st.column_config.TextColumn("Hạng mục chi phí", required=False),
                    "description": st.column_config.TextColumn("Diễn giải"),
                    "unit": st.column_config.TextColumn("Đơn vị"),
                    "unit_price": st.column_config.TextColumn("Đơn giá (VND)", required=False),
                    "quantity": st.column_config.NumberColumn("Số lượng", min_value=0),
                    "times": st.column_config.NumberColumn("Số lần", min_value=1),
                    "price_per_pax": st.column_config.TextColumn("Giá/Pax", disabled=True),
                    "total_display": st.column_config.TextColumn("Thực tế (VND)", disabled=True),
                    "est_display": st.column_config.TextColumn("Dự toán (VND)", disabled=True),
                    "diff_display": st.column_config.TextColumn("Chênh lệch", disabled=True),
                    "total_val": st.column_config.NumberColumn("Hidden", disabled=True),
                    "est_val": st.column_config.NumberColumn("Hidden", disabled=True),
                    "diff_val": st.column_config.NumberColumn("Hidden", disabled=True),
                },
                disabled=not is_act_editable, # Khóa nếu không được phép sửa
                column_order=("category", "description", "unit", "unit_price", "quantity", "times", "price_per_pax", "total_display", "est_display", "diff_display"),
                use_container_width=True,
                hide_index=True,
                key="editor_act"
            )
            
            # --- AUTO-UPDATE CALCULATION (ACTUAL) ---
            if is_act_editable:
                # Tự động cập nhật khi dữ liệu thay đổi
                df_new_act = edited_act.copy()
                
                def clean_vnd_act_auto(x):
                    if isinstance(x, str):
                        return float(x.replace('.', '').replace(',', '').replace(' VND', '').strip())
                    return float(x) if x else 0.0
                
                df_new_act['unit_price'] = df_new_act['unit_price'].apply(clean_vnd_act_auto)
                df_new_act['quantity'] = pd.to_numeric(df_new_act['quantity'], errors='coerce').fillna(0)
                if 'times' not in df_new_act.columns: df_new_act['times'] = 1
                df_new_act['times'] = pd.to_numeric(df_new_act['times'], errors='coerce').fillna(1)
                
                # So sánh với dữ liệu cũ
                cols_check_act = ['category', 'description', 'unit', 'unit_price', 'quantity', 'times']
                df_old_act = st.session_state.act_df_temp.copy()
                if 'times' not in df_old_act.columns: df_old_act['times'] = 1
                
                # Reset index và fillna để so sánh
                df_new_check_act = df_new_act[cols_check_act].reset_index(drop=True).fillna(0)
                df_old_check_act = df_old_act[cols_check_act].reset_index(drop=True).fillna(0)
                
                if len(df_new_check_act) != len(df_old_check_act) or not df_new_check_act.equals(df_old_check_act):
                    st.session_state.act_df_temp = df_new_act[cols_check_act]
                    st.rerun()

            act_total_cost = 0
            if not edited_act.empty:
                # Parse unit_price
                # [FIX] Handle case where a cell is None, which becomes the string 'None' after astype(str)
                cleaned_prices_act = edited_act['unit_price'].astype(str).str.replace('.', '', regex=False).str.replace(' VND', '', regex=False).str.strip()
                p_price_act = cleaned_prices_act.apply(lambda x: float(x) if x and x.lower() != 'none' else 0.0)
                # Ensure 'times' column exists and is numeric before accessing it
                # Use .get() with a default Series to handle cases where 'times' might be missing
                times_col_act = edited_act.get('times', pd.Series([1.0] * len(edited_act), index=edited_act.index)).fillna(1).astype(float) # type: ignore
                act_total_cost = (edited_act['quantity'] * p_price_act * times_col_act).sum()
            # TỔNG CHI PHÍ THỰC TẾ = Hóa đơn + Phát sinh (Nhập tay)
            final_act_cost = act_total_cost + total_inv

            # --- TỔNG KẾT QUYẾT TOÁN ---
            st.divider()
            st.markdown("### ⚖️ Tổng kết & Đối chiếu")
            
            c_sum1, c_sum2, c_sum3 = st.columns(3)
            c_sum1.metric("Tổng Chi phí (HĐ + Phát sinh)", format_vnd(final_act_cost), help="Tổng chi phí thực tế của tour")
            c_sum2.metric("Tổng UNC (Đã thanh toán)", format_vnd(total_unc), help="Tổng tiền đã chi ra từ tài khoản")
            
            diff = total_unc - final_act_cost
            if diff == 0:
                c_sum3.success("✅ Đã khớp (UNC = Chi phí)")
            elif diff > 0:
                c_sum3.warning(f"⚠️ UNC dư: {format_vnd(diff)}")
            else:
                c_sum3.error(f"⚠️ Thiếu UNC: {format_vnd(abs(diff))}")
            
            # Lợi nhuận = Tổng doanh thu (Dự toán) - Tổng chi
            final_profit = est_final_sale - final_act_cost
            
            st.markdown(f"""<div class="profit-summary-card">
                <h3>TỔNG DOANH THU - TỔNG CHI = LỢI NHUẬN</h3>
                <div class="formula">{format_vnd(est_final_sale)} - {format_vnd(final_act_cost)} = <span class="result">{format_vnd(final_profit)} VND</span></div>
            </div>
            """, unsafe_allow_html=True)

            # --- EXPORT EXCEL (ACT) ---
            st.write("")
            # Prepare Data for Export
            df_exp_act = edited_act.copy()
            if 'times' not in df_exp_act.columns: df_exp_act['times'] = 1
            df_exp_act['times'] = df_exp_act.get('times', pd.Series([1.0] * len(df_exp_act), index=df_exp_act.index)).fillna(1).astype(float)

            # Clean numbers

            def clean_num_act(x): # type: ignore
                if isinstance(x, str):
                    return float(x.replace('.', '').replace(',', '').replace(' VND', '').strip())
                return float(x) if x else 0.0
            
            df_exp_act['unit_price'] = df_exp_act['unit_price'].apply(clean_num_act)
            df_exp_act['quantity'] = pd.to_numeric(df_exp_act['quantity'], errors='coerce').fillna(0)
            df_exp_act['total_amount'] = df_exp_act['quantity'] * df_exp_act['unit_price'] * df_exp_act['times']
            df_exp_act['price_per_pax'] = df_exp_act['total_amount'] / guest_cnt_act
            
            # --- COMPARISON LOGIC ---
            # [CODE MỚI] Sử dụng lại est_lookup đã tạo ở trên để tính cột Dự toán và Chênh lệch cho Excel
            def get_est_val_exp(row): # type: ignore
                k = (str(row['category']).strip().lower(), str(row['description']).strip().lower()) # type: ignore
                return est_lookup.get(k, 0.0)

            df_exp_act['est_amount'] = df_exp_act.apply(get_est_val_exp, axis=1)
            df_exp_act['diff_amount'] = df_exp_act['est_amount'] - df_exp_act['total_amount'] # type: ignore
            
            def classify_item(row):
                if row['diff_amount'] < 0: return "Vượt chi"
                elif row['diff_amount'] > 0: return "Tiết kiệm"
                return ""

            df_exp_act['Ghi chú'] = df_exp_act.apply(classify_item, axis=1)

            # Rename
            df_exp_act = df_exp_act.rename(columns={
                'category': 'Hạng mục', 
                'description': 'Diễn giải', 
                'unit': 'Đơn vị', 
                'unit_price': 'Đơn giá', 
                'quantity': 'Số lượng', 
                'times': 'Số lần',
                'price_per_pax': 'Giá/Pax',
                'total_amount': 'Thực tế',
                'est_amount': 'Dự toán',
                'diff_amount': 'Chênh lệch'
            })
            
            # [REQUEST 1] Bỏ cột 'Số lần' -> Keep it
            cols_to_export = ['Hạng mục', 'Diễn giải', 'Đơn vị', 'Đơn giá', 'Số lượng', 'Số lần', 'Giá/Pax', 'Dự toán', 'Thực tế', 'Chênh lệch', 'Ghi chú']
            df_exp_act_filtered = df_exp_act[cols_to_export]

            # [REQUEST 2] Tách thành 2 bảng: Chi phí trong dự toán và chi phí phát sinh
            df_in_est = df_exp_act_filtered[df_exp_act_filtered['Dự toán'] > 0].copy()
            df_extra_cost = df_exp_act_filtered[df_exp_act_filtered['Dự toán'] == 0].copy()

            buffer_act = io.BytesIO()
            file_ext_act = "xlsx"
            mime_type_act = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            
            try:
                with pd.ExcelWriter(buffer_act, engine='xlsxwriter') as writer:
                    workbook: Any = writer.book
                    worksheet = workbook.add_worksheet('QuyetToan')
                    
                    # Styles (Copied and adapted)
                    company_name_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#D84315'}) # Orange for Act
                    company_info_fmt = workbook.add_format({'font_size': 10, 'italic': True, 'font_color': '#424242'})
                    title_fmt = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter', 'font_color': '#BF360C', 'bg_color': '#FBE9E7', 'border': 1})
                    
                    header_fmt = workbook.add_format({'bold': True, 'fg_color': '#D84315', 'font_color': 'white', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                    body_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'text_wrap': True, 'font_size': 10})
                    body_center_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'align': 'center', 'font_size': 10})
                    money_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'num_format': '#,##0', 'font_size': 10})
                    
                    # Summary Styles
                    sum_header_bg_fmt = workbook.add_format({'bold': True, 'bg_color': '#FFF3E0', 'border': 1, 'font_color': '#E65100', 'align': 'center', 'valign': 'vcenter'})
                    sum_label_fmt = workbook.add_format({'bold': True, 'align': 'left', 'border': 1, 'bg_color': '#FAFAFA'})
                    sum_val_fmt = workbook.add_format({'num_format': '#,##0', 'align': 'right', 'border': 1})
                    sum_val_bold_fmt = workbook.add_format({'bold': True, 'num_format': '#,##0', 'align': 'right', 'border': 1})
                    
                    # [CODE MỚI] Format màu đỏ cho dòng âm
                    alert_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'text_wrap': True, 'font_size': 10, 'font_color': '#D32F2F'})
                    alert_money_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'num_format': '#,##0', 'font_size': 10, 'font_color': '#D32F2F'})

                    # [CODE MỚI] Format cho tiêu đề các bảng chi phí
                    section_title_fmt = workbook.add_format({'bold': True, 'font_size': 12, 'font_color': '#004D40', 'bg_color': '#E0F2F1', 'border': 1, 'align': 'center'})

                    # 1. Company Info
                    if comp['logo_b64_str']:
                        try:
                            logo_data = base64.b64decode(comp['logo_b64_str'])
                            image_stream = io.BytesIO(logo_data)
                            img = Image.open(image_stream)
                            w, h = img.size
                            scale = 60 / h if h > 0 else 0.5
                            image_stream.seek(0)
                            worksheet.insert_image('A1', 'logo.png', {'image_data': image_stream, 'x_scale': scale, 'y_scale': scale, 'x_offset': 5, 'y_offset': 5})
                        except: pass
                    
                    worksheet.write('B1', comp['name'], company_name_fmt)
                    worksheet.write('B2', f"ĐC: {comp['address']}", company_info_fmt)
                    worksheet.write('B3', f"SĐT: {comp['phone']}", company_info_fmt)
                    
                    # 2. Tour Info
                    worksheet.merge_range('A5:I5', "BẢNG QUYẾT TOÁN CHI PHÍ TOUR", title_fmt)
                    
                    t_info_dict = dict(zip(tour_info_act.keys(), tour_info_act))
                    worksheet.write('A7', "Tên đoàn:", sum_label_fmt)
                    worksheet.merge_range('B7:D7', t_info_dict.get('tour_name',''), sum_val_fmt)
                    worksheet.write('E7', "Mã đoàn:", sum_label_fmt)
                    worksheet.merge_range('F7:I7', t_info_dict.get('tour_code',''), sum_val_fmt)
                    
                    worksheet.write('A8', "Khách hàng:", sum_label_fmt)
                    worksheet.merge_range('B8:D8', f"{t_info_dict.get('customer_name','')} - {t_info_dict.get('customer_phone','')}", sum_val_fmt)
                    worksheet.write('E8', "Sales:", sum_label_fmt)
                    worksheet.merge_range('F8:I8', t_info_dict.get('sale_name',''), sum_val_fmt)
                    
                    worksheet.write('A9', "Thời gian:", sum_label_fmt)
                    worksheet.merge_range('B9:D9', f"{t_info_dict.get('start_date','')} - {t_info_dict.get('end_date','')}", sum_val_fmt)
                    worksheet.write('E9', "Số khách:", sum_label_fmt)
                    worksheet.merge_range('F9:I9', t_info_dict.get('guest_count',0), sum_val_fmt)

                    # 3. Table Header & Body (MODIFIED)
                    current_row = 10 # Bắt đầu từ dòng 11

                    # --- Bảng 1: Chi phí trong dự toán ---
                    if not df_in_est.empty:
                        worksheet.merge_range(current_row, 0, current_row, len(df_in_est.columns)-1, "CHI PHÍ TRONG DỰ TOÁN", section_title_fmt)
                        current_row += 1
                        for col_num, value in enumerate(df_in_est.columns):
                            worksheet.write(current_row, col_num, value, header_fmt)
                        for row_idx in range(len(df_in_est)):
                            diff_val = df_in_est.iloc[row_idx, 7] # Chênh lệch
                            is_negative = isinstance(diff_val, (int, float)) and diff_val < 0
                            for col_idx in range(len(df_in_est.columns)):
                                val = df_in_est.iloc[row_idx, col_idx]
                                if col_idx == 2: fmt = body_center_fmt
                                elif col_idx in [3, 4, 5, 6, 7, 8, 9]: fmt = money_fmt
                                else: fmt = body_fmt
                                if is_negative:
                                    if col_idx in [3, 4, 5, 6, 7, 8, 9]: fmt = alert_money_fmt
                                    else: fmt = alert_fmt
                                if pd.isna(val): val = ""
                                worksheet.write(current_row + 1 + row_idx, col_idx, val, fmt)
                        current_row += len(df_in_est) + 1

                    # Thêm dòng trống
                    current_row += 1

                    # --- Bảng 2: Chi phí phát sinh ngoài dự toán ---
                    if not df_extra_cost.empty:
                        worksheet.merge_range(current_row, 0, current_row, len(df_extra_cost.columns)-1, "CHI PHÍ PHÁT SINH NGOÀI DỰ TOÁN", section_title_fmt)
                        current_row += 1
                        for col_num, value in enumerate(df_extra_cost.columns):
                            worksheet.write(current_row, col_num, value, header_fmt)
                        for row_idx in range(len(df_extra_cost)):
                            # Chi phí phát sinh luôn là âm (vượt chi)
                            is_negative = True
                            for col_idx in range(len(df_extra_cost.columns)):
                                val = df_extra_cost.iloc[row_idx, col_idx]
                                if col_idx == 2: fmt = body_center_fmt
                                elif col_idx in [3, 4, 5, 6, 7, 8, 9]: fmt = money_fmt
                                else: fmt = body_fmt
                                if is_negative:
                                    if col_idx in [3, 4, 5, 6, 7, 8, 9]: fmt = alert_money_fmt
                                    else: fmt = alert_fmt
                                if pd.isna(val): val = ""
                                worksheet.write(current_row + 1 + row_idx, col_idx, val, fmt)
                        current_row += len(df_extra_cost) + 1
                    
                    # 4. Summary
                    sum_row = current_row + 1
                    
                    worksheet.merge_range(sum_row, 0, sum_row, 3, "TỔNG KẾT QUYẾT TOÁN", sum_header_bg_fmt)
                    
                    # [CODE MỚI] Hiển thị đầy đủ thông tin tài chính
                    # 1. Tổng doanh thu
                    worksheet.write(sum_row+1, 0, "1. Tổng doanh thu:", sum_label_fmt)
                    worksheet.merge_range(sum_row+1, 1, sum_row+1, 3, est_final_sale, sum_val_bold_fmt)
                    
                    # 2. Tổng chi phí (Bảng kê + Hóa đơn ngoài)
                    worksheet.write(sum_row+2, 0, "2. Tổng chi phí thực tế:", sum_label_fmt)
                    worksheet.merge_range(sum_row+2, 1, sum_row+2, 3, final_act_cost, sum_val_bold_fmt)
                    
                    # 3. Lợi nhuận
                    worksheet.write(sum_row+3, 0, "3. Lợi nhuận thực tế:", sum_label_fmt)
                    profit_fmt = workbook.add_format({'bold': True, 'num_format': '#,##0', 'align': 'right', 'border': 1, 'bg_color': '#C8E6C9', 'font_color': '#1B5E20'})
                    worksheet.merge_range(sum_row+3, 1, sum_row+3, 3, final_profit, profit_fmt)
                    
                    # Note nhỏ về chi phí ngoài
                    if total_inv > 0:
                        worksheet.write(sum_row+4, 0, f"(Bao gồm {format_vnd(total_inv)} hóa đơn phát sinh ngoài bảng kê)", workbook.add_format({'italic': True, 'font_size': 9}))
                    
                    # Column Widths
                    worksheet.set_column('A:A', 25)
                    worksheet.set_column('B:B', 40)
                    worksheet.set_column('C:C', 10)
                    worksheet.set_column('D:I', 15)

            except Exception as e:
                # If xlsxwriter fails, fall back to a simple CSV export
                st.error(f"⚠️ Lỗi khi tạo file Excel: {e}. Đã chuyển sang xuất file CSV.")
                buffer_act.seek(0)
                buffer_act.truncate()
                df_exp_act_filtered.to_csv(buffer_act, index=False, encoding='utf-8-sig')
                file_ext_act = "csv"
                mime_type_act = "text/csv"

            clean_t_name_act = re.sub(r'[\\/*?:"<>|]', "", tour_info_act['tour_name'] if tour_info_act else "Tour") # type: ignore
            st.download_button(label=f"📥 Tải Bảng Quyết Toán ({file_ext_act.upper()})", data=buffer_act.getvalue(), file_name=f"QuyetToan_{clean_t_name_act}.{file_ext_act}", mime=mime_type_act, use_container_width=True)

            def save_act_logic():
                run_query("DELETE FROM tour_items WHERE tour_id=? AND item_type='ACT'", (tour_id_act,), commit=True)
                data_to_insert = []
                query = """INSERT INTO tour_items (tour_id, item_type, category, description, unit, quantity, unit_price, total_amount, times)
                           VALUES (?, 'ACT', ?, ?, ?, ?, ?, ?, ?)"""

                for _, row in edited_act.iterrows():
                    if row['category'] or row['description']: # type: ignore
                        u_price = float(str(row['unit_price']).replace('.', '').replace(' VND', '').strip()) if row['unit_price'] else 0 # type: ignore
                        # Handle times safely
                        t_times = row.get('times', 1) # type: ignore
                        if pd.isna(t_times): t_times = 1
                        total_row = row['quantity'] * u_price * t_times # type: ignore

                        data_to_insert.append((
                            tour_id_act,
                            row['category'],
                            row['description'],
                            row['unit'],
                            row['quantity'],
                            u_price, # type: ignore
                            total_row,
                            t_times
                        ))

                if data_to_insert:
                    run_query_many(query, data_to_insert)

            if is_act_editable:
                if st.button("💾 LƯU QUYẾT TOÁN", type="primary", use_container_width=True):
                    save_act_logic()
                    st.success("Đã lưu quyết toán!"); time.sleep(1); st.rerun()
            else:
                st.info("🔒 Chỉ Admin mới được chỉnh sửa quyết toán.")
            
            st.divider()
            if st.button("✅ HOÀN THÀNH TOUR (Chuyển vào Lịch sử)", type="primary", use_container_width=True, key="complete_tour_btn"):
                run_query("UPDATE tours SET status='completed' WHERE id=?", (tour_id_act,), commit=True)
                st.success("Đã hoàn thành tour! Tour đã được chuyển sang tab Lịch sử.")
                time.sleep(1)

                st.rerun()

    # ---------------- TAB 4: LỊCH SỬ TOUR ----------------
    with tab_hist:
        st.subheader("📜 Lịch sử Tour đã hoàn thành")
        completed_tours = [t for t in all_tours if t['status'] == 'completed']
        
        if completed_tours:
            df_hist = pd.DataFrame([dict(t) for t in completed_tours])
            st.dataframe(
                df_hist[['tour_code', 'tour_name', 'start_date', 'end_date', 'guest_count', 'sale_name']],
                column_config={
                    "tour_code": "Mã Tour",
                    "tour_name": "Tên Tour",
                    "start_date": "Ngày đi",
                    "end_date": "Ngày về",
                    "guest_count": "Số khách",
                    "sale_name": "Sales"
                },
                use_container_width=True,
                hide_index=True
            )
            
            st.divider()
            st.write("🛠️ Thao tác:")
            hist_opts = {f"[{t['tour_code']}] {t['tour_name']}": t['id'] for t in completed_tours} # type: ignore
            sel_hist = st.selectbox("Chọn tour để xem lại hoặc mở lại:", list(hist_opts.keys()), key="sel_hist_tour")
            if sel_hist:
                tid_hist = hist_opts[sel_hist] # type: ignore
                if st.button("🔓 Mở lại Tour (Chuyển về Đang chạy)", key="reopen_tour_btn"):
                    run_query("UPDATE tours SET status='running' WHERE id=?", (tid_hist,), commit=True)
                    st.success("Đã mở lại tour! Kiểm tra lại bên tab Quyết toán.")
                    time.sleep(1)
                    st.rerun()
        else:
            st.info("Chưa có tour nào trong lịch sử.")

    # ---------------- TAB 3: TỔNG HỢP LỢI NHUẬN ----------------
    with tab_rpt:
        st.subheader("📈 Tổng Hợp Lợi Nhuận & Doanh Số")
        
        # Lọc theo thời gian
        rpt_df = pd.DataFrame([dict(r) for r in all_tours])
        if not rpt_df.empty:
            rpt_df['dt'] = pd.to_datetime(rpt_df['start_date'], format='%d/%m/%Y', errors='coerce') # type: ignore
            rpt_df = rpt_df.dropna(subset=['dt'])
            
            rpt_df['Month'] = rpt_df['dt'].apply(lambda x: x.strftime('%m/%Y'))
            rpt_df['Quarter'] = rpt_df['dt'].apply(lambda x: f"Q{(x.month-1)//3+1}/{x.year}")
            rpt_df['Year'] = rpt_df['dt'].apply(lambda x: x.strftime('%Y'))
            
            # --- PRE-FETCH DATA FOR PERFORMANCE ---
            all_items = run_query("SELECT tour_id, item_type, total_amount FROM tour_items")
            items_map = {} 
            if all_items:
                for item in all_items:
                    tid = item['tour_id']
                    itype = item['item_type']
                    amt = item['total_amount'] or 0
                    if tid not in items_map: items_map[tid] = {'EST': 0, 'ACT': 0}
                    items_map[tid][itype] += amt
            
            # Tính toán chỉ số cho từng tour
            results = []
            for _, t in rpt_df.iterrows():
                tid = t['id'] # type: ignore
                costs = items_map.get(tid, {'EST': 0, 'ACT': 0})
                est_cost = costs['EST']
                act_cost = costs['ACT']
                
                p_pct = t.get('est_profit_percent', 0) or 0
                t_pct = t.get('est_tax_percent', 0) or 0

                # Tính doanh thu (Ưu tiên giá chốt tay)
                final_price_manual = float(t.get('final_tour_price', 0) or 0)
                child_price_manual = float(t.get('child_price', 0) or 0)
                final_qty = float(t.get('final_qty', 0) or 0)
                child_qty = float(t.get('child_qty', 0) or 0)
                if final_qty == 0: final_qty = float(t.get('guest_count', 1))
                
                manual_revenue = (final_price_manual * final_qty) + (child_price_manual * child_qty)
                
                if manual_revenue > 0:
                    final_sale = manual_revenue
                else:
                    profit_est_val = est_cost * (p_pct/100)
                    final_sale = (est_cost + profit_est_val) * (1 + t_pct/100)

                net_revenue = final_sale / (1 + t_pct/100) if (1 + t_pct/100) != 0 else final_sale
                
                real_profit = net_revenue - act_cost
                
                results.append({
                    **t.to_dict(),
                    "Tên Đoàn": t['tour_name'], # type: ignore
                    "Sales": t['sale_name'], # type: ignore
                    "Ngày đi": t['start_date'], # type: ignore
                    "Doanh Thu Thuần": net_revenue,
                    "Chi Phí TT": act_cost,
                    "Lợi Nhuận TT": real_profit,
                })
            
            res_df = pd.DataFrame(results)

            # --- UI CONTROLS ---
            c_type, c_period, c_val = st.columns(3)
            report_type = c_type.selectbox("Loại báo cáo:", ["Theo Tour (Chi tiết)", "Theo Sales (Tổng hợp)"])
            period_type = c_period.selectbox("Xem theo:", ["Tháng", "Quý", "Năm"])
            
            period_options = []
            period_col = 'Month'
            if period_type == "Tháng":
                period_col = 'Month'
                period_options = sorted(res_df['Month'].unique(), reverse=True)
            elif period_type == "Quý":
                period_col = 'Quarter'
                period_options = sorted(res_df['Quarter'].unique(), reverse=True)
            else:
                period_col = 'Year'
                period_options = sorted(res_df['Year'].unique(), reverse=True)
            
            selected_period = c_val.selectbox("Chọn thời gian:", ["Tất cả"] + period_options)
            
            # Filter
            if selected_period != "Tất cả":
                res_df = res_df[res_df[period_col] == selected_period]
            
            if report_type == "Theo Tour (Chi tiết)":
                res_df['Tỷ suất LN'] = res_df.apply(lambda x: (x['Lợi Nhuận TT']/x['Doanh Thu Thuần']*100) if x['Doanh Thu Thuần'] else 0, axis=1)
                
                c_sum1, c_sum2 = st.columns(2)
                c_sum1.metric("Tổng Lợi Nhuận", format_vnd(res_df['Lợi Nhuận TT'].sum()))
                c_sum2.metric("Tổng Doanh Thu", format_vnd(res_df['Doanh Thu Thuần'].sum()))
                
                st.dataframe(
                    res_df[['Tên Đoàn', 'Sales', 'Ngày đi', 'Doanh Thu Thuần', 'Chi Phí TT', 'Lợi Nhuận TT', 'Tỷ suất LN']],
                    column_config={
                        "Doanh Thu Thuần": st.column_config.NumberColumn(format="%d VND"),
                        "Chi Phí TT": st.column_config.NumberColumn(format="%d VND"),
                        "Lợi Nhuận TT": st.column_config.NumberColumn(format="%d VND"),
                        "Tỷ suất LN": st.column_config.NumberColumn(format="%.2f %%"),
                    },
                    use_container_width=True,
                    hide_index=True
                )
                
                # Chuẩn bị dữ liệu xuất Excel
                df_export = res_df[['Tên Đoàn', 'Sales', 'Ngày đi', 'Doanh Thu Thuần', 'Chi Phí TT', 'Lợi Nhuận TT', 'Tỷ suất LN']].copy()
                file_name_rpt = f"BaoCao_LoiNhuan_Tour_{selected_period.replace('/', '_')}.xlsx"
            else: # Theo Sales
                df_sales = res_df.groupby('Sales').agg({
                    'Doanh Thu Thuần': 'sum',
                    'Chi Phí TT': 'sum',
                    'Lợi Nhuận TT': 'sum',
                    'id': 'count'
                }).reset_index()
                df_sales.columns = ["Nhân viên Sales", "Doanh Thu Thuần", "Chi Phí TT", "Lợi Nhuận TT", "Số Tour"]
                df_sales['Tỷ suất LN'] = df_sales.apply(lambda x: (x['Lợi Nhuận TT']/x['Doanh Thu Thuần']*100) if x['Doanh Thu Thuần'] else 0, axis=1)
                df_sales = df_sales.sort_values('Lợi Nhuận TT', ascending=False)
                
                st.markdown(f"##### 🏆 Bảng xếp hạng Sales ({selected_period})")
                if not df_sales.empty:
                    best = df_sales.iloc[0]
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Top Sales", best['Nhân viên Sales'], delta=format_vnd(best['Lợi Nhuận TT']))
                    c2.metric("Tổng Doanh Số", format_vnd(df_sales['Doanh Thu Thuần'].sum()))
                    c3.metric("Tổng Lợi Nhuận", format_vnd(df_sales['Lợi Nhuận TT'].sum()))
                    
                    st.bar_chart(df_sales.set_index("Nhân viên Sales")[['Doanh Thu Thuần', 'Lợi Nhuận TT']])
                
                st.dataframe(
                    df_sales,
                    column_config={
                        "Doanh Thu Thuần": st.column_config.NumberColumn(format="%d VND"),
                        "Chi Phí TT": st.column_config.NumberColumn(format="%d VND"),
                        "Lợi Nhuận TT": st.column_config.NumberColumn(format="%d VND"),
                        "Tỷ suất LN": st.column_config.NumberColumn(format="%.2f %%"),
                        "Số Tour": st.column_config.NumberColumn(format="%d"),
                    },
                    use_container_width=True,
                    hide_index=True
                )
                
                # Chuẩn bị dữ liệu xuất Excel
                df_export = df_sales.copy()
                file_name_rpt = f"BaoCao_DoanhSo_Sales_{selected_period.replace('/', '_')}.xlsx"

            # --- TÍNH NĂNG XUẤT EXCEL ---
            st.write("")
            buffer_rpt = io.BytesIO()
            with pd.ExcelWriter(buffer_rpt, engine='xlsxwriter') as writer:
                df_export.to_excel(writer, index=False, sheet_name='Report')
                workbook = writer.book
                worksheet = writer.sheets['Report']
                
                # Định dạng
                header_fmt = workbook.add_format({'bold': True, 'fg_color': '#2E7D32', 'font_color': 'white', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
                body_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter'})
                money_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'num_format': '#,##0'})
                pct_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter', 'num_format': '0.00"%"'})
                
                # Áp dụng định dạng header
                for col_num, value in enumerate(df_export.columns):
                    worksheet.write(0, col_num, value, header_fmt)
                
                # Áp dụng định dạng body
                for row_idx in range(len(df_export)):
                    for col_idx in range(len(df_export.columns)):
                        val = df_export.iloc[row_idx, col_idx]
                        col_name = df_export.columns[col_idx]
                        
                        fmt = body_fmt
                        if col_name in ['Doanh Thu Thuần', 'Chi Phí TT', 'Lợi Nhuận TT']: fmt = money_fmt
                        elif col_name == 'Tỷ suất LN': fmt = pct_fmt
                        
                        if pd.isna(val): val = ""
                        worksheet.write(row_idx + 1, col_idx, val, fmt)
                
                worksheet.set_column('A:A', 25)
                worksheet.set_column('B:Z', 18)

            st.download_button("📥 Xuất báo cáo Excel", buffer_rpt.getvalue(), file_name_rpt, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("Chưa có dữ liệu tour.")

def render_customer_management():
    st.title("🤝 Quản Lý Khách Hàng")
    
    # Lấy thông tin user hiện tại để lọc
    current_user_info_cust = st.session_state.get("user_info", {})
    current_user_name_cust = current_user_info_cust.get('name', 'N/A')
    current_user_role_cust = current_user_info_cust.get('role')
    
    tab_list, tab_add = st.tabs(["📋 Danh sách khách hàng", "➕ Thêm khách hàng"])
    
    with tab_add:
        with st.form("add_cust_form"):
            st.subheader("Thêm khách hàng mới")
            c1, c2 = st.columns(2)
            name = c1.text_input("Tên khách hàng (*)", placeholder="Nguyễn Văn A")
            phone = c2.text_input("Số điện thoại", placeholder="090...")
            email = c1.text_input("Email", placeholder="abc@gmail.com")
            addr = c2.text_input("Địa chỉ")
            note = st.text_area("Ghi chú")
            
            if st.form_submit_button("Lưu khách hàng", type="primary"):
                if name:
                    add_row_to_table('customers', {
                        'name': name, 'phone': phone, 'email': email, 'address': addr, 'notes': note,
                        'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'sale_name': current_user_name_cust
                    })
                    st.success("Đã thêm khách hàng mới!"); time.sleep(1); st.rerun()
                else:
                    st.warning("Vui lòng nhập tên khách hàng.")

    with tab_list:
        # Search bar
        search_term = st.text_input("🔍 Tìm kiếm", placeholder="Nhập tên hoặc số điện thoại...")
        
        query = "SELECT * FROM customers"
        params = []

        # Base filter for sales role
        if current_user_role_cust == 'sale':
            query += " WHERE sale_name=?"
            params.append(current_user_name_cust)

        # Additional filter for search term
        if search_term:
            if "WHERE" in query:
                query += " AND (name LIKE ? OR phone LIKE ?)"
            else:
                query += " WHERE name LIKE ? OR phone LIKE ?"
            params.extend([f"%{search_term}%", f"%{search_term}%"])
        query += " ORDER BY id DESC"
        
        customers = run_query(query, tuple(params))
        
        if customers:
            # Display as dataframe for overview
            df_cust = pd.DataFrame([dict(r) for r in customers])
            st.dataframe(
                df_cust[['name', 'phone', 'email', 'address', 'notes']],
                column_config={
                    "name": "Tên khách hàng",
                    "phone": "SĐT",
                    "email": "Email",
                    "address": "Địa chỉ",
                    "notes": "Ghi chú"
                },
                use_container_width=True,
                hide_index=True
            )
            
            st.divider()
            st.markdown("##### 🛠️ Chỉnh sửa thông tin")
            
            cust_options = {f"{c['name']} - {c['phone']}": c['id'] for c in customers} # type: ignore
            selected_cust = st.selectbox("Chọn khách hàng để sửa/xóa:", list(cust_options.keys()))
            
            if selected_cust:
                cid = cust_options[selected_cust] # type: ignore
                c_info = next((c for c in customers if c['id'] == cid), None)
                
                if c_info:
                    with st.form(f"edit_cust_{cid}"):
                        c1, c2 = st.columns(2)
                        n_name = c1.text_input("Tên", value=c_info['name']) # type: ignore
                        n_phone = c2.text_input("SĐT", value=c_info['phone']) # type: ignore
                        n_email = c1.text_input("Email", value=c_info['email']) # type: ignore
                        n_addr = c2.text_input("Địa chỉ", value=c_info['address']) # type: ignore
                        n_note = st.text_area("Ghi chú", value=c_info['notes']) # type: ignore
                        
                        c_save, c_del = st.columns(2)
                        if c_save.form_submit_button("💾 Cập nhật"):
                            run_query("UPDATE customers SET name=?, phone=?, email=?, address=?, notes=? WHERE id=?", 
                                      (n_name, n_phone, n_email, n_addr, n_note, cid), commit=True)
                            st.success("Đã cập nhật!"); time.sleep(0.5); st.rerun()
                        
                        if c_del.form_submit_button("🗑️ Xóa khách hàng"):
                            run_query("DELETE FROM customers WHERE id=?", (cid,), commit=True)
                            st.success("Đã xóa!"); time.sleep(0.5); st.rerun()
        else:
            st.info("Chưa có khách hàng nào.")

def render_hr_management():
    st.title("👥 Quản Lý Nhân Sự & Tài Khoản")
    
    if (st.session_state.user_info or {}).get('role') not in ['admin', 'admin_f1']:
        st.warning("⛔ Khu vực này chỉ dành cho Admin hoặc Admin F1. Vui lòng liên hệ quản trị viên.")
    else:
        tab_list, tab_req = st.tabs(["📋 Danh sách tài khoản", "📝 Duyệt đăng ký mới"])
        
        with tab_list:
            st.subheader("Danh sách tài khoản hệ thống")
            
            # Lấy dữ liệu users
            users = run_query("SELECT id, username, role, status FROM users ORDER BY id ASC")
            if users:
                df_users = pd.DataFrame([dict(r) for r in users])
                original_df = df_users.copy()
                
                # Xác định các quyền có thể gán
                role_options = ["admin", "admin_f1", "user", "sale", "accountant"]
                if (st.session_state.user_info or {}).get('role') == 'admin_f1':
                    role_options = ["admin_f1", "user", "sale", "accountant"] # Admin F1 không thể tạo admin chính

                # Hiển thị bảng
                edited_df = st.data_editor(
                    df_users,
                    column_config={
                        "id": st.column_config.NumberColumn("ID", width="small", disabled=True),
                        "username": st.column_config.TextColumn("Tên đăng nhập", width="medium", disabled=True),
                        "role": st.column_config.SelectboxColumn("Quyền hạn", options=role_options, required=True, width="medium"),
                        "status": st.column_config.SelectboxColumn("Trạng thái", options=["approved", "pending", "blocked"], required=True, width="medium")
                    },
                    use_container_width=True,
                    hide_index=True
                )
                
                if st.button("💾 Lưu thay đổi phân quyền", type="primary"):
                    if not original_df.equals(edited_df):
                        with st.spinner("Đang cập nhật..."):
                            current_user_role = (st.session_state.user_info or {}).get('role')
                            # Iterate through the edited dataframe
                            for index, row in edited_df.iterrows():
                                original_row = original_df.loc[index]# type: ignore
                                # Check if the row has changed
                                if not row.equals(original_row):
                                    user_id = row['id'] # type: ignore
                                    username = row['username'] # type: ignore
                                    new_role = row['role'] # type: ignore
                                    new_status = row['status'] # type: ignore
                                    original_role = original_row['role'] # type: ignore

                                    # Prevent changing the main admin
                                    if username == 'admin':
                                        st.warning("Không thể thay đổi quyền của tài khoản 'admin' chính.")
                                        continue
                                    
                                    # Prevent F1 from editing a full admin
                                    if current_user_role == 'admin_f1' and original_role == 'admin':
                                        st.warning(f"Bạn không có quyền chỉnh sửa tài khoản admin '{username}'.")
                                        continue
                                    
                                    run_query(
                                        "UPDATE users SET role=?, status=? WHERE id=?",
                                        (new_role, new_status, user_id),
                                        commit=True
                                    )
                        st.success("Đã cập nhật thành công!")
                        time.sleep(1); st.rerun()
                    else:
                        st.toast("Không có thay đổi nào.")
                
                st.divider()
                st.markdown("##### 🗑️ Xóa tài khoản")
                # Loại bỏ admin chính ra khỏi danh sách xóa để tránh lỗi
                del_options = [u['username'] for u in users if u['username'] != 'admin'] # type: ignore
                user_to_del = st.selectbox("Chọn tài khoản cần xóa:", del_options, key="sel_del_u")
                
                if st.button("Xác nhận xóa tài khoản", type="primary", key="btn_del_u"):
                    if user_to_del:
                        # Kiểm tra quyền trước khi xóa
                        user_to_del_info = run_query("SELECT role FROM users WHERE username=?", (user_to_del,), fetch_one=True)
                        current_user_role = (st.session_state.user_info or {}).get('role')

                        if current_user_role == 'admin_f1' and user_to_del_info and user_to_del_info['role'] == 'admin': # type: ignore
                            st.error(f"Bạn không có quyền xóa tài khoản admin '{user_to_del}'.")
                        else:
                            run_query("DELETE FROM users WHERE username=?", (user_to_del,), commit=True)
                            st.success(f"Đã xóa tài khoản {user_to_del}!")
                            time.sleep(1); st.rerun()
            else:
                st.info("Chưa có tài khoản nào.")

        with tab_req:
            st.subheader("Yêu cầu đăng ký chờ duyệt")
            pending = run_query("SELECT * FROM users WHERE status='pending'")
            if pending:
                for p in pending:
                    with st.container(border=True):
                        c1, c2, c3 = st.columns([2, 1, 1])
                        c1.write(f"User: **{p['username']}**") # type: ignore
                        if c2.button("✔ Duyệt", key=f"hr_app_{p['id']}", use_container_width=True): # type: ignore
                            run_query("UPDATE users SET status='approved' WHERE id=?", (p['id'],), commit=True) # type: ignore
                            st.success("Đã duyệt!"); time.sleep(0.5); st.rerun()
                        if c3.button("✖ Xóa", key=f"hr_del_{p['id']}", use_container_width=True): # type: ignore
                            run_query("DELETE FROM users WHERE id=?", (p['id'],), commit=True) # type: ignore
                            st.success("Đã xóa!"); time.sleep(0.5); st.rerun()
            else:
                st.info("Hiện không có yêu cầu nào.")

def render_search_module():
    st.title("🔍 Tra cứu thông tin hệ thống")
    
    # Lấy thông tin user hiện tại để lọc
    current_user_info = st.session_state.get("user_info", {})
    current_user_name = current_user_info.get('name', 'N/A')
    current_user_role = current_user_info.get('role')

    query = st.text_input("Nhập từ khóa tìm kiếm", placeholder="Nhập Mã Tour, Số Hóa Đơn, Mã Vé, Mã Chi Phí, hoặc Tên Khách...", help="Hệ thống sẽ tìm trong Tour, Hóa đơn, UNC và Vé máy bay")
        
    if query:
        st.divider()
        term = f"%{query.strip()}%"
        found_any = False
        
        # 1. TÌM TRONG TOUR
        tour_sql = "SELECT * FROM tours WHERE (tour_code LIKE ? OR tour_name LIKE ?)"
        tour_params = [term, term]
        if current_user_role == 'sale':
            tour_sql += " AND sale_name=?"
            tour_params.append(current_user_name)
            
        tours = run_query(tour_sql, tuple(tour_params))
        if tours:
            found_any = True
            st.subheader(f"📦 Tìm thấy {len(tours)} Tour")
            for t in tours:
                with st.expander(f"Tour: {t['tour_name']} (Mã: {t['tour_code']})", expanded=True):
                    c1, c2, c3 = st.columns(3) # type: ignore
                    c1.write(f"**Sales:** {t['sale_name']}") # type: ignore
                    c2.write(f"**Ngày:** {t['start_date']} - {t['end_date']}") # type: ignore
                    c3.write(f"**Khách:** {t['guest_count']}") # type: ignore
                    
                    est = run_query("SELECT SUM(total_amount) as t FROM tour_items WHERE tour_id=? AND item_type='EST'", (t['id'],), fetch_one=True) # type: ignore
                    act = run_query("SELECT SUM(total_amount) as t FROM tour_items WHERE tour_id=? AND item_type='ACT'", (t['id'],), fetch_one=True) # type: ignore
                    est_val = est['t'] if isinstance(est, sqlite3.Row) and est['t'] else 0 # type: ignore
                    act_val = act['t'] if isinstance(act, sqlite3.Row) and act['t'] else 0 # type: ignore
                    
                    st.info(f"💰 Dự toán: {format_vnd(est_val)} | 💸 Quyết toán: {format_vnd(act_val)}")

        # 2. TÌM TRONG KHÁCH HÀNG (MỚI)
        cust_sql = "SELECT * FROM customers WHERE (name LIKE ? OR phone LIKE ?)"
        cust_params = [term, term]
        if current_user_role == 'sale':
            cust_sql += " AND sale_name=?"
            cust_params.append(current_user_name)
            
        custs = run_query(cust_sql, tuple(cust_params))
        if custs:
            found_any = True
            st.subheader(f"👥 Tìm thấy {len(custs)} Khách hàng")
            for c in custs:
                with st.expander(f"Khách hàng: {c['name']} - {c['phone']}", expanded=True):
                    st.write(f"**Email:** {c['email']}")
                    st.write(f"**Địa chỉ:** {c['address']}")
                    st.write(f"**Ghi chú:** {c['notes']}")

        # 3. TÌM TRONG HÓA ĐƠN / UNC
        invs = run_query("SELECT * FROM invoices WHERE invoice_number LIKE ? OR cost_code LIKE ? OR memo LIKE ? ORDER BY date DESC", (term, term, term))
        if invs:
            found_any = True
            st.subheader(f"💰 Tìm thấy {len(invs)} Hóa đơn / UNC")
            
            for inv in invs:
                icon = "💸" if "UNC" in (inv['invoice_number'] or "") else "📄"
                i_num = inv['invoice_number'] if inv['invoice_number'] else "(Không số)" # type: ignore
                label = f"{icon} {inv['date']} | {i_num} | {format_vnd(inv['total_amount'])} | {inv['memo']}" # type: ignore
                
                with st.expander(label):
                    c_info, c_file = st.columns([1, 1])
                    with c_info:
                        st.markdown(f"**Bên bán:** {inv['seller_name']}") # type: ignore
                        st.markdown(f"**Bên mua:** {inv['buyer_name']}") # type: ignore
                        st.markdown(f"**Tổng tiền:** {format_vnd(inv['total_amount'])}") # type: ignore
                        st.markdown(f"**Mã chi phí:** `{inv['cost_code']}`") # type: ignore
                        st.caption(f"Trạng thái: {inv['status']}") # type: ignore
                    
                    with c_file:
                        file_path = inv['file_path'] # type: ignore
                        if file_path and os.path.exists(file_path):
                            # The 'file_path' from the database is a Google Drive link, not a local path.
                            # The original code to check os.path.exists(file_path) and open it is incorrect.
                            # We should just provide the link.
                            st.link_button("🔗 Mở file trên Google Drive", file_path, use_container_width=True)

        if not found_any:
            st.warning("📭 Không tìm thấy dữ liệu nào phù hợp.")

def main():
    if not st.session_state.logged_in:
        render_login_page(comp)
        return

    module, menu = render_sidebar(comp)

    # --- HEADER CHÍNH ---
    l_html = f'<img src="data:image/png;base64,{comp["logo_b64_str"]}" class="company-logo-img">' if comp['logo_b64_str'] else ''
    st.markdown(f'''
    <div class="company-header-container">
        {l_html}
        <div class="company-info-text">
            <h1>{comp['name']}</h1>
            <p>📍 {comp['address']}</p>
            <p>📞 {comp['phone']}</p>
        </div>
    </div>
    ''', unsafe_allow_html=True)

    if module == "💰 Kiểm Soát Chi Phí":
        render_cost_control(menu)
    elif module == "💳 Quản Lý Công Nợ":
        render_debt_management()
    elif module == "🔖 Quản Lý Booking":
        render_booking_management()
    elif module == "📦 Quản Lý Tour ":
        render_tour_management()
    elif module == "🤝 Quản Lý Khách Hàng":
        render_customer_management()
    elif module == "👥 Quản Lý Nhân Sự":
        render_hr_management()
    elif module == "🔍 Tra cứu thông tin":
        render_search_module()

if __name__ == "__main__":
    main()
