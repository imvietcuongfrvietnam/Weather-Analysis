import os

# ===========================
# POSTGRESQL CONFIGURATION
# ===========================

# Kubernetes Service Name: airflow-postgresql.airflow.svc.cluster.local
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DATABASE = os.getenv("POSTGRES_DB", "weather_db")

# Thông tin user/pass lấy từ biến môi trường (khớp với file deploy)
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass")

# Cấu hình Driver và Table
POSTGRES_DRIVER = "org.postgresql.Driver"
FORECAST_TABLE = "weather_forecasts"

# JDBC URL cho Spark
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

# ===========================
# SPARK JDBC CONFIGURATION
# ===========================
# Các config này sẽ được dùng trực tiếp trong Spark DataFrameWriter/Reader

SPARK_POSTGRES_CONFIG = {
    "url": POSTGRES_JDBC_URL,
    "driver": POSTGRES_DRIVER,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "dbtable": FORECAST_TABLE,
}

# ===========================
# SQL SCHEMA (Dùng cho Init Database)
# ===========================

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
"""

# ===========================
# HELPER FUNCTIONS
# ===========================

def print_config():
    """In ra cấu hình hiện tại (để debug)"""
    print("\n" + "="*80)
    print("🐘 POSTGRESQL CONFIGURATION")
    print("="*80)
    print(f"Endpoint:     {POSTGRES_HOST}:{POSTGRES_PORT}")
    print(f"Database:     {POSTGRES_DATABASE}")
    print(f"User:         {POSTGRES_USER}")
    print(f"JDBC URL:     {POSTGRES_JDBC_URL}")
    print(f"Target Table: {FORECAST_TABLE}")
    print("="*80 + "\n")


def validate_config(test_connection: bool = False):
    """
    Validate PostgreSQL configuration
    """
    print("\n🔍 Validating PostgreSQL Configuration...")
    
    # Check required fields
    required_fields = {
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_DATABASE": POSTGRES_DATABASE,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD
    }
    
    for name, value in required_fields.items():
        if not value:
            raise ValueError(f"{name} is not set!")
    
    # Cảnh báo môi trường
    if POSTGRES_HOST == "localhost":
        print("   ⚠️  WARNING: Using localhost PostgreSQL (Local mode)")
    else:
        print(f"   ✅ Using Remote/K8s PostgreSQL: {POSTGRES_HOST}")

    if test_connection:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DATABASE,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=5
            )
            conn.close()
            print("   ✅ PostgreSQL connection test successful!")
        except Exception as e:
            print(f"   ⚠️  PostgreSQL connection test failed: {e}")

    print_config()
    return True

if __name__ == "__main__":
    validate_config()