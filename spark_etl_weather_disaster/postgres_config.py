"""
PostgreSQL Configuration
Cấu hình kết nối PostgreSQL để lưu kết quả dự đoán từ SparkML

HƯỚNG DẪN SETUP:
1. Cài đặt PostgreSQL server (local hoặc remote)
2. Tạo database và user
3. Cập nhật các thông tin kết nối bên dưới

SETUP PostgreSQL LOCAL (Docker):
docker run --name postgres-weather \
  -e POSTGRES_DB=weather_db \
  -e POSTGRES_USER=weather_user \
  -e POSTGRES_PASSWORD=weather_pass \
  -p 5432:5432 \
  -d postgres:15

Sau đó chạy SQL script để tạo bảng (xem LSTM_FORECAST_GUIDE.md)
"""

# ===========================
# POSTGRESQL CONFIGURATION
# ===========================

# PostgreSQL Server Connection
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "weather_db"
POSTGRES_USER = "weather_user"
POSTGRES_PASSWORD = "weather_pass"

# JDBC URL for Spark
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

# PostgreSQL Driver (cần có trong Spark classpath)
POSTGRES_DRIVER = "org.postgresql.Driver"

# ===========================
# TABLE CONFIGURATION
# ===========================

# Bảng lưu kết quả dự đoán thời tiết
FORECAST_TABLE = "weather_forecasts"

# Schema cho bảng forecasts
FORECAST_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS weather_forecasts (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_datetime TIMESTAMP NOT NULL,
    temperature_celsius DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    wind_direction_deg DOUBLE PRECISION,
    model_version VARCHAR(50),
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confidence_score DOUBLE PRECISION,
    UNIQUE(city, forecast_date, forecast_datetime)
);

CREATE INDEX IF NOT EXISTS idx_forecast_city_date ON weather_forecasts(city, forecast_date);
CREATE INDEX IF NOT EXISTS idx_forecast_datetime ON weather_forecasts(forecast_datetime);
"""

# ===========================
# SPARK JDBC CONFIGURATION
# ===========================

def get_spark_jdbc_config():
    """
    Trả về cấu hình JDBC cho Spark
    
    Returns:
        dict: JDBC configuration dictionary
    """
    return {
        "url": POSTGRES_JDBC_URL,
        "driver": POSTGRES_DRIVER,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "dbtable": FORECAST_TABLE,
    }


def get_spark_jdbc_properties():
    """
    Trả về properties cho Spark JDBC connection
    
    Returns:
        dict: JDBC properties
    """
    return {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": POSTGRES_DRIVER,
    }


# ===========================
# HELPER FUNCTIONS
# ===========================

def print_config():
    """In ra cấu hình hiện tại (để debug)"""
    print("\n" + "="*80)
    print("🐘 POSTGRESQL CONFIGURATION")
    print("="*80)
    print(f"Host:         {POSTGRES_HOST}")
    print(f"Port:         {POSTGRES_PORT}")
    print(f"Database:     {POSTGRES_DATABASE}")
    print(f"User:         {POSTGRES_USER}")
    print(f"Password:     {'*' * len(POSTGRES_PASSWORD)}")
    print(f"JDBC URL:     {POSTGRES_JDBC_URL}")
    print(f"Table:        {FORECAST_TABLE}")
    print("="*80 + "\n")


def validate_config(test_connection: bool = False):
    """
    Validate PostgreSQL configuration
    
    Args:
        test_connection: If True, test actual connection to PostgreSQL server
        
    Returns:
        bool: True if valid, raises ValueError if invalid
    """
    print("\n🔍 Validating PostgreSQL Configuration...")
    
    # Check required fields
    if not POSTGRES_HOST:
        raise ValueError("POSTGRES_HOST is not set!")
    
    if not POSTGRES_DATABASE:
        raise ValueError("POSTGRES_DATABASE is not set!")
    
    if not POSTGRES_USER:
        raise ValueError("POSTGRES_USER is not set!")
    
    if not POSTGRES_PASSWORD:
        raise ValueError("POSTGRES_PASSWORD is not set!")
    
    # Warn if using defaults
    if POSTGRES_HOST == "localhost":
        print("   ⚠️  WARNING: Using localhost PostgreSQL server")
        print("      Make sure PostgreSQL is running locally or update config for production")
    
    if POSTGRES_USER == "weather_user":
        print("   ⚠️  WARNING: Using default PostgreSQL credentials")
        print("      Change these for production!")
    
    print("   ✅ Configuration validation passed!")
    
    # Optional: Test actual connection
    if test_connection:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DATABASE,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.close()
            print("   ✅ PostgreSQL connection validated!")
        except ImportError:
            print("   💡 psycopg2 not installed. Skipping connection test.")
            print("   💡 Install with: pip install psycopg2-binary")
        except Exception as e:
            print(f"   ⚠️  PostgreSQL connection test failed: {e}")
            print("   💡 Make sure PostgreSQL is running and credentials are correct")
    
    print_config()
    return True


if __name__ == "__main__":
    # Test configuration
    validate_config(test_connection=False)
    
    print("\nExample JDBC config:")
    print(get_spark_jdbc_config())

