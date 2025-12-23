"""
PostgreSQL Writer - Ghi dữ liệu dự đoán vào PostgreSQL
Save weather forecast predictions to PostgreSQL database
Updated: Adjusted schema to match current features (No precipitation)
"""

from pyspark.sql import DataFrame
import sys
import os

# --- IMPORT CONFIG ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import config
except ImportError:
    # Fallback Config (Cập nhật theo thông số đã test thành công)
    class Config:
        POSTGRES_HOST = "weather-postgresql.default.svc.cluster.local"
        POSTGRES_PORT = "5432"
        POSTGRES_DB = "weather_db"
        POSTGRES_USER = "weather_user"
        POSTGRES_PASSWORD = "weather_pass"
        POSTGRES_TABLE = "weather_predictions"
        POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        POSTGRES_PROPERTIES = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        POSTGRES_WRITE_MODE = "append"
    config = Config()

class PostgresWriter:
    """Write forecast predictions to PostgreSQL database"""
    
    def __init__(self):
        # Kiểm tra xem config có đủ thông tin không
        if not hasattr(config, 'POSTGRES_JDBC_URL'):
            print("⚠️  Warning: Config thiếu thông tin PostgreSQL (POSTGRES_JDBC_URL)")
            self.jdbc_url = None
        else:
            self.jdbc_url = config.POSTGRES_JDBC_URL
            self.properties = config.POSTGRES_PROPERTIES
            self.table = config.POSTGRES_TABLE
            self.write_mode = config.POSTGRES_WRITE_MODE
        
    def write_predictions(self, df: DataFrame, table_name: str = None):
        """
        Ghi DataFrame dự đoán vào PostgreSQL
        """
        if not self.jdbc_url:
            print("❌ Cannot write: Missing PostgreSQL configuration.")
            return False

        table = table_name or self.table
        
        print(f"\n💾 Writing predictions to PostgreSQL...")
        print(f"   URL: {self.jdbc_url}")
        print(f"   Table: {table}")
        
        try:
            # Ghi vào PostgreSQL qua JDBC
            # Spark sẽ tự động tạo bảng nếu chưa có (dựa trên schema của DataFrame)
            # Tuy nhiên, tốt nhất là bảng nên được tạo trước với schema chuẩn.
            df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table=table,
                    mode=self.write_mode,
                    properties=self.properties
                )
            
            # Đếm số dòng (action này trigger việc ghi dữ liệu thực tế)
            # Lưu ý: df.count() có thể tốn thời gian nếu df chưa cache
            print(f"   ✅ Successfully wrote records to PostgreSQL")
            return True
            
        except Exception as e:
            print(f"   ⚠️  PostgreSQL Write Failed: {e}")
            print(f"   💡 Nguyên nhân có thể: Server chưa bật, sai mật khẩu, lỗi mạng, hoặc Database '{config.POSTGRES_DB}' chưa được tạo.")
            return False
    
    def write_predictions_safe(self, df: DataFrame, table_name: str = None):
        """
        Phiên bản an toàn: Không gây crash chương trình nếu lỗi DB
        """
        try:
            return self.write_predictions(df, table_name)
        except Exception:
            return False
    
    def create_table_sql(self) -> str:
        """
        Tạo câu lệnh SQL để tạo bảng (Tham khảo - Dùng để chạy thủ công nếu cần)
        Lưu ý: Schema này phải khớp với DataFrame đầu ra của ML Job
        """
        sql = f"""
CREATE TABLE IF NOT EXISTS {self.table} (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    city VARCHAR(100),
    
    -- Actual values (Dữ liệu thật)
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    
    -- Predicted values (Dữ liệu dự báo)
    prediction_temperature DOUBLE PRECISION,
    prediction_humidity DOUBLE PRECISION,
    prediction_pressure DOUBLE PRECISION,
    prediction_wind_speed DOUBLE PRECISION,
    prediction_wind_direction DOUBLE PRECISION,
    prediction_weather_desc VARCHAR(100),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo Index để query nhanh cho Dashboard
CREATE INDEX IF NOT EXISTS idx_datetime ON {self.table} (datetime);
CREATE INDEX IF NOT EXISTS idx_city ON {self.table} (city);
        """
        return sql.strip()

if __name__ == "__main__":
    writer = PostgresWriter()
    print("--- SQL CREATE TABLE (REFERENCE) ---")
    print(writer.create_table_sql())
    print("------------------------------------")