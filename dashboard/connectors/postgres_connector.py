"""
PostgreSQL Connector
Kết nối và lấy dữ liệu dự báo từ PostgreSQL
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
    # Fallback cấu hình nếu chạy test lẻ
    class Config:
        POSTGRES_HOST = "localhost"
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
            query = f"SELECT DISTINCT city FROM {self.table} ORDER BY city"
            cursor.execute(query)
            cities = [row[0] for row in cursor.fetchall()]
            return cities
        except Exception as e:
            print(f"Error fetching cities: {e}")
            return []
        finally:
            cursor.close()
            self.connection_pool.putconn(conn)

    def get_latest_predictions(self, city_name, limit=168):
        """
        Lấy dữ liệu dự báo.
        QUAN TRỌNG: Mapping tên cột trong DB sang tên cột mà Code UI của bạn cần.
        """
        if not self.connection_pool:
            if not self.connect(): return None

        conn = self.connection_pool.getconn()
        try:
            # Code UI của bạn cần: prediction_temp_celsius
            # DB đang có: prediction_temperature
            # => Dùng SQL AS để đổi tên
            query = f"""
            SELECT 
                datetime,
                city,
                -- Dữ liệu thực tế (Actual)
                temperature AS temp_celsius,
                humidity AS humidity_pct,
                wind_speed AS wind_speed_kmh,
                
                -- Dữ liệu dự báo (Prediction) - Mapping cho khớp UI
                prediction_temperature AS prediction_temp_celsius,
                prediction_humidity AS prediction_humidity_pct,
                prediction_wind_speed AS prediction_wind_speed_kmh,
                prediction_weather_desc AS prediction_weather_condition,
                
                -- Tạo cột giả cho Mưa (vì DB hiện tại chưa có, tránh lỗi UI)
                0.0 AS prediction_precipitation_mm,
                0.0 AS precipitation_mm
                
            FROM {self.table}
            WHERE city = %s
            ORDER BY datetime DESC
            LIMIT %s
            """
            
            df = pd.read_sql(query, conn, params=(city_name, limit))
            return df
            
        except Exception as e:
            print(f"❌ Error fetching forecast: {e}")
            return None
        finally:
            self.connection_pool.putconn(conn)

# =============================================================================

if __name__ == "__main__":
    print(f"🧪 Testing Postgres connection to {config.POSTGRES_HOST}:{config.POSTGRES_PORT}...")
    db = PostgresConnector()
    if db.connect():
        print("✅ Connected!")
        cities = db.get_available_cities()
        print(f"Found cities: {cities}")
        
        if cities:
            df = db.get_latest_predictions(cities[0])
            if df is not None:
                print("\nSample Data (First 3 rows):")
                print(df[['datetime', 'prediction_temp_celsius']].head(3))
                print("\nColumns:", df.columns.tolist())
    else:
        print("❌ Failed.")