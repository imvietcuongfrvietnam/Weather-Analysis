import pytest
import redis
import psycopg2
import sys
import os

# --- 1. SETUP PATH ĐỂ IMPORT CONFIG CHÍNH ---
# Lấy đường dẫn thư mục cha (dashboard/)
current_test_dir = os.path.dirname(os.path.abspath(__file__))
dashboard_root_dir = os.path.dirname(current_test_dir)

# Thêm vào sys.path để Python tìm thấy 'config.py' của App
if dashboard_root_dir not in sys.path:
    sys.path.insert(0, dashboard_root_dir)

try:
    # Import config từ dashboard/config.py (File thật dùng cho App)
    import config
    print(f"\n📂 Loading Main App Config from: {config.__file__}")
except ImportError as e:
    pytest.fail(f"❌ Không tìm thấy file 'config.py' trong thư mục dashboard/: {e}")

# --- 2. TEST CASES ---

def test_redis_connection():
    """
    Kiểm tra kết nối Redis sử dụng thông số từ config.py
    """
    # Lấy thông số từ file config thật
    host = config.REDIS_HOST
    port = config.REDIS_PORT
    db = config.REDIS_DB
    password = config.REDIS_PASSWORD
    
    print(f"\n🔌 Testing Redis at {host}:{port} (DB: {db})...")
    
    try:
        # Kết nối thử
        r = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            password=password, 
            socket_timeout=3
        )
        
        # 1. Ping server
        assert r.ping() is True
        print("   ✅ Ping: OK")
        
        # 2. Test Quyền Ghi/Đọc
        test_key = "test_connection_check"
        r.set(test_key, "ok")
        val = r.get(test_key).decode("utf-8")
        assert val == "ok"
        r.delete(test_key)
        print("   ✅ Write/Read: OK")
        
    except Exception as e:
        msg = str(e)
        if "Connection refused" in msg:
            msg += f"\n💡 GỢI Ý: Có thể Port {port} chưa được Forward?\n   Chạy: kubectl port-forward svc/weather-redis {port}:6379"
        pytest.fail(f"❌ Redis Failed: {msg}")

def test_postgres_connection():
    """
    Kiểm tra kết nối Postgres sử dụng thông số từ config.py
    """
    # Lấy thông số từ file config thật
    host = config.POSTGRES_HOST
    port = config.POSTGRES_PORT
    dbname = config.POSTGRES_DB
    user = config.POSTGRES_USER
    password = config.POSTGRES_PASSWORD
    
    print(f"\n🐘 Testing Postgres at {host}:{port} (DB: {dbname})...")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password
        )
        cur = conn.cursor()
        
        # 1. Test Query cơ bản
        cur.execute("SELECT 1;")
        assert cur.fetchone()[0] == 1
        print("   ✅ Connection: OK")
        
        # 2. Kiểm tra bảng dữ liệu (Table Existence)
        table_name = config.POSTGRES_TABLE
        cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
        
        if cur.fetchone()[0]:
            print(f"   ✅ Table '{table_name}' found.")
            # Đếm số dòng
            cur.execute(f"SELECT count(*) FROM {table_name};")
            count = cur.fetchone()[0]
            print(f"   📊 Rows: {count}")
        else:
            print(f"   ⚠️ Table '{table_name}' NOT found (Nhưng kết nối DB OK).")
            
    except Exception as e:
        msg = str(e)
        if "password authentication failed" in msg:
            msg += f"\n💡 GỢI Ý: Mật khẩu trong config là '{password}'. Nếu sai, hãy set lại biến môi trường POSTGRES_PASSWORD."
        pytest.fail(f"❌ Postgres Failed: {msg}")
        
    finally:
        if conn: conn.close()