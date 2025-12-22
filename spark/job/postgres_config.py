import os

# ===========================
# WEATHER POSTGRESQL CONFIGURATION
# ===========================

# SỬA TẠI ĐÂY: Dùng FQDN để gọi từ namespace 'airflow' sang namespace 'default'
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "weather-postgresql.default.svc.cluster.local")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DATABASE = os.getenv("POSTGRES_DB", "weather_db")

# Thông tin user/pass (Đảm bảo khớp với Database bạn đã khởi tạo trong namespace default)
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass")

POSTGRES_DRIVER = "org.postgresql.Driver"
FORECAST_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")

# JDBC URL sẽ tự động cập nhật theo HOST mới
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

SPARK_POSTGRES_CONFIG = {
    "url": POSTGRES_JDBC_URL,
    "driver": POSTGRES_DRIVER,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "dbtable": FORECAST_TABLE,
    # Thêm tham số này để Spark ML ghi dữ liệu mượt hơn
    "batchsize": "1000",
    "reWriteBatchedInserts": "true"
}

def print_config():
    """In ra cấu hình hiện tại để debug trên Airflow Logs"""
    print("\n" + "="*80)
    print("🐘 WEATHER POSTGRESQL CONFIGURATION (CROSS-NAMESPACE)")
    print("="*80)
    print(f"Host FQDN:    {POSTGRES_HOST}")
    print(f"Database:     {POSTGRES_DATABASE}")
    print(f"JDBC URL:     {POSTGRES_JDBC_URL}")
    print("="*80 + "\n")

if __name__ == "__main__":
    print_config()