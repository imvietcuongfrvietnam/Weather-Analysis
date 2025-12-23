"""
PostgreSQL Writer - Ghi dữ liệu dự đoán vào PostgreSQL
Save weather forecast predictions to PostgreSQL database
Updated: Changed mode to 'overwrite' to handle schema changes automatically.
"""

from pyspark.sql import DataFrame
import sys
import os

# --- IMPORT CONFIG ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import config
except ImportError:
    class Config:
        # Fallback Config nếu không import được
        POSTGRES_JDBC_URL = "jdbc:postgresql://weather-postgresql.default.svc.cluster.local:5432/weather_db"
        POSTGRES_PROPERTIES = {
            "user": "weather_user",
            "password": "weather_pass",
            "driver": "org.postgresql.Driver"
        }
        # QUAN TRỌNG: Dùng overwrite để tự động recreate table khi schema thay đổi
        POSTGRES_TABLE = "weather_predictions"
    config = Config()

class PostgresWriter:
    """Write forecast predictions to PostgreSQL database"""
    
    def __init__(self):
        self.jdbc_url = getattr(config, 'POSTGRES_JDBC_URL', None)
        self.properties = getattr(config, 'POSTGRES_PROPERTIES', {})
        self.table = getattr(config, 'POSTGRES_TABLE', 'weather_predictions')
        
        # ⚠️ CHUYỂN SANG 'overwrite':
        # Spark sẽ Drop table cũ và Create table mới khớp với DataFrame.
        # Giúp giải quyết lỗi "Column not found" tự động.
        self.write_mode = "overwrite" 
        
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
        print(f"   Mode: {self.write_mode} (Will drop & recreate table)")
        
        try:
            # Ghi vào PostgreSQL qua JDBC
            df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table=table,
                    mode=self.write_mode, # overwrite
                    properties=self.properties
                )
            
            print(f"   ✅ Successfully wrote records to PostgreSQL")
            return True
            
        except Exception as e:
            print(f"   ⚠️  PostgreSQL Write Failed: {e}")
            print(f"   💡 Nguyên nhân: {e}")
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
        SQL tham khảo (Spark overwrite sẽ tự làm bước này, nhưng giữ lại để debug)
        Đã cập nhật thêm wind_direction
        """
        sql = f"""
CREATE TABLE IF NOT EXISTS {self.table} (
    datetime TIMESTAMP,
    city VARCHAR(100),
    
    -- Actual values
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION, -- ✅ Mới thêm
    
    -- Predicted values
    prediction_temperature DOUBLE PRECISION,
    prediction_humidity DOUBLE PRECISION,
    prediction_pressure DOUBLE PRECISION,
    prediction_wind_speed DOUBLE PRECISION,
    prediction_wind_direction DOUBLE PRECISION, -- ✅ Mới thêm
    prediction_weather_desc VARCHAR(100),
    
    created_at TIMESTAMP
);
        """
        return sql.strip()

if __name__ == "__main__":
    writer = PostgresWriter()
    print("--- SQL SCHEMA REFERENCE ---")
    print(writer.create_table_sql())