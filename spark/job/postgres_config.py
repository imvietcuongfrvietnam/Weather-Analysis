import os

# ===========================
# WEATHER POSTGRESQL CONFIGURATION
# ===========================

# Tên Service K8s bạn đã đặt trong file YAML (Dùng nội bộ trong cùng namespace default)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "weather-postgresql")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DATABASE = os.getenv("POSTGRES_DB", "weather_db")

# Thông tin user/pass lấy từ biến môi trường (Khớp chính xác với Deployment YAML)
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass")

# Cấu hình Driver và Table nghiệp vụ cho ML
POSTGRES_DRIVER = "org.postgresql.Driver"
# Tên bảng này nên khớp với config của Job ML
FORECAST_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")

# JDBC URL cho Spark kết nối nội bộ Cluster
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

# ===========================
# SPARK JDBC CONFIGURATION
# ===========================
# Dùng config này trong Spark ML: df.write.format("jdbc").options(**SPARK_POSTGRES_CONFIG)

SPARK_POSTGRES_CONFIG = {
    "url": POSTGRES_JDBC_URL,
    "driver": POSTGRES_DRIVER,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "dbtable": FORECAST_TABLE,
}

# ===========================
# HELPER FUNCTIONS
# ===========================

def print_config():
    """In ra cấu hình hiện tại để debug trên Airflow Logs"""
    print("\n" + "="*80)
    print("🐘 WEATHER POSTGRESQL CONFIGURATION (DATA WAREHOUSE)")
    print("="*80)
    print(f"Host:         {POSTGRES_HOST}")
    print(f"Database:     {POSTGRES_DATABASE}")
    print(f"User:         {POSTGRES_USER}")
    print(f"Table:        {FORECAST_TABLE}")
    print(f"JDBC URL:     {POSTGRES_JDBC_URL}")
    print("="*80 + "\n")

def validate_config():
    """Kiểm tra các biến môi trường bắt buộc"""
    print("\n🔍 Validating Weather PostgreSQL Configuration...")
    for var_name, value in [("HOST", POSTGRES_HOST), ("DB", POSTGRES_DATABASE), ("USER", POSTGRES_USER)]:
        if not value:
            print(f"   ❌ Missing config: POSTGRES_{var_name}")
            return False
    print("   ✅ Configuration looks good for K8s environment!")
    return True

if __name__ == "__main__":
    print_config()
    validate_config()