"""
PostgreSQL Connector
Kết nối và lấy dữ liệu dự báo từ PostgreSQL
Updated: Added Wind Direction columns
"""

import psycopg2
from psycopg2 import pool
import pandas as pd
import sys
import os

# --- SETUP IMPORT CONFIG ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import config
except ImportError:
    # Fallback cấu hình (Đã cập nhật khớp với hệ thống chuẩn)
    class Config:
        POSTGRES_HOST = "localhost" # Hoặc weather-postgresql... nếu chạy trong pod
        POSTGRES_PORT = "5432"
        POSTGRES_DB = "weather_db"
        POSTGRES_USER = "weather_user"
        POSTGRES_PASSWORD = "weather_pass"
        POSTGRES_TABLE = "weather_predictions"
    config = Config()

class PostgresConnector:
    """Connect to PostgreSQL and fetch forecast data"""
    
    def __init__(self):
        self.host = config.POSTGRES_HOST
        self.port = config.POSTGRES_PORT
        self.database = config.POSTGRES_DB
        self.user = config.POSTGRES_USER
        self.password = config.POSTGRES_PASSWORD
        self.table = config.POSTGRES_TABLE
        self.connection_pool = None
        
    def connect(self) -> bool:
        """Thiết lập kết nối (Connection Pool)"""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            return True
        except Exception as e:
            print(f"❌ PostgreSQL connection error: {e}")
            return False

    def close(self):
        """Đóng kết nối"""
        if self.connection_pool:
            self.connection_pool.closeall()
            print("🔒 PostgreSQL connection pool closed.")

    def is_connected(self):
        """Kiểm tra trạng thái kết nối"""
        return self.connection_pool is not None

    def get_available_cities(self):
        """Lấy danh sách các thành phố có trong DB"""
        if not self.connection_pool:
            if not self.connect(): return []
            
        conn = self.connection_pool.getconn()
        cursor = conn.cursor()
        try:
            # Chỉ lấy các thành phố có dữ liệu
            query = f"SELECT DISTINCT city FROM {self.table} ORDER BY city"
            cursor.execute(query)
            cities = [row[0] for row in cursor.fetchall()]
            return cities
        except Exception as e:
            print(f"Error fetching cities: {e}")
            return []
        finally:
            if cursor: cursor.close()
            if conn: self.connection_pool.putconn(conn)

    def get_latest_predictions(self, city_name, limit=336):
        """
        Lấy dữ liệu dự báo (Quá khứ + Tương lai).
        Limit 336 = 14 ngày (7 ngày cũ + 7 ngày mới)
        """
        if not self.connection_pool:
            if not self.connect(): return None

        conn = self.connection_pool.getconn()
        try:
            # Query lấy dữ liệu và đổi tên cột (Alias) cho khớp với Frontend
            query = f"""
            SELECT 
                datetime,
                city,
                
                -- Dữ liệu thực tế (Actual)
                temperature AS temp_celsius,
                humidity AS humidity_pct,
                wind_speed AS wind_speed_kmh,
                wind_direction AS wind_direction,
                
                -- Dữ liệu dự báo (Prediction)
                prediction_temperature AS prediction_temp_celsius,
                prediction_humidity AS prediction_humidity_pct,
                prediction_wind_speed AS prediction_wind_speed_kmh,
                prediction_wind_direction AS prediction_wind_direction,
                prediction_weather_desc AS prediction_weather_condition
                
            FROM {self.table}
            WHERE city = %s
            ORDER BY datetime DESC
            LIMIT %s
            """
            
            # Dùng pandas đọc SQL trực tiếp
            df = pd.read_sql(query, conn, params=(city_name, limit))
            return df
            
        except Exception as e:
            print(f"❌ Error fetching forecast: {e}")
            return None
        finally:
            if conn: self.connection_pool.putconn(conn)

# =============================================================================

if __name__ == "__main__":
    print(f"🧪 Testing Postgres connection to {config.POSTGRES_HOST}...")
    db = PostgresConnector()
    if db.connect():
        print("✅ Connected!")
        cities = db.get_available_cities()
        print(f"Found cities: {cities}")
        
        if cities:
            # Test lấy dữ liệu của thành phố đầu tiên
            df = db.get_latest_predictions(cities[0], limit=5)
            if df is not None:
                print("\nSample Data (First 5 rows):")
                # In ra các cột quan trọng để kiểm tra
                cols_to_show = ['datetime', 'temp_celsius', 'prediction_temp_celsius', 'wind_direction']
                available_cols = [c for c in cols_to_show if c in df.columns]
                print(df[available_cols])
                print("\nAll Columns:", df.columns.tolist())
    else:
        print("❌ Failed.")