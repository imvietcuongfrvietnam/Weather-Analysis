"""
Weather Forecasting ML - Configuration
Cấu hình cho hệ thống dự đoán thời tiết
Updated: Standardized Column Names & Service Hosts
"""

import os

# ========================================
# 1. MINIO CONFIGURATION
# ========================================
# SỬA TẠI ĐÂY: Trỏ về namespace default
MINIO_HOST_FQDN = "weather-minio.default.svc.cluster.local"
MINIO_PORT = "9000"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", f"{MINIO_HOST_FQDN}:{MINIO_PORT}")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")

# Paths - Đảm bảo khớp với output của main_etl.py
MINIO_INPUT_PATH = f"s3a://{MINIO_BUCKET}/enriched/weather/"
MINIO_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/ml/forecasts/"
MINIO_MODEL_PATH = f"s3a://{MINIO_BUCKET}/ml/models/"

SPARK_S3_CONFIG = {
    # Thêm http:// để Spark hiểu giao thức kết nối
    "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}",
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1" # Tránh lỗi DNS timeout
}

# ========================================
# 2. POSTGRESQL CONFIGURATION
# ========================================
# SỬA TẠI ĐÂY: Trỏ về namespace default
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "weather-postgres.default.svc.cluster.local")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_forecast")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

POSTGRES_WRITE_MODE = "append"

# ========================================
# 3. ML FEATURE CONFIGURATION (QUAN TRỌNG)
# ========================================
# Tên cột phải KHỚP CHÍNH XÁC với dữ liệu từ main_etl.py (Normalize Step)

# Các biến liên tục (Số)
CONTINUOUS_FEATURES = [
    'temperature',      # Cũ: temp_celsius
    'humidity',         # Cũ: humidity_pct
    'pressure',         # Cũ: pressure_hpa
    'wind_speed',       # Cũ: wind_speed_kmh
    'precipitation_mm'  # Giữ nguyên
]

# Các biến phân loại (Chữ)
CATEGORICAL_FEATURES = ['weather_desc'] # Cũ: weather_condition

ALL_TARGET_FEATURES = CONTINUOUS_FEATURES + CATEGORICAL_FEATURES

# Feature Engineering Params
LAG_HOURS = [1, 3, 6, 12, 24]
ROLLING_WINDOWS = [6, 12, 24]

# Model Configuration
TRAIN_TEST_SPLIT = 0.8
RANDOM_SEED = 42

GBT_PARAMS = {'maxIter': 50, 'maxDepth': 5, 'stepSize': 0.1, 'subsamplingRate': 0.8}
RF_REGRESSION_PARAMS = {'numTrees': 50, 'maxDepth': 8, 'minInstancesPerNode': 2}
RF_CLASSIFICATION_PARAMS = {'numTrees': 50, 'maxDepth': 8, 'minInstancesPerNode': 2}

# Mapping Feature -> Algorithm
MODEL_SELECTION = {
    'temperature': 'GBT',
    'humidity': 'GBT',
    'pressure': 'GBT',
    'wind_speed': 'RandomForest',
    'precipitation_mm': 'RandomForest',
    'weather_desc': 'RandomForestClassifier'
}

# ========================================
# 4. LOCAL OUTPUT
# ========================================
LOCAL_OUTPUT_DIR = "./output"
LOCAL_MODEL_DIR = "./models_output"
LOCAL_PLOTS_DIR = "./plots_output"

# ========================================
# 5. DATA QUALITY
# ========================================
MIN_TRAINING_RECORDS = 50 # Giảm xuống để dễ test
MIN_DAYS_HISTORY = 1
MAX_MISSING_PCT = 0.2

def print_config():
    """Print current configuration"""
    print("\n" + "="*60)
    print("⚙️  CONFIGURATION LOADED")
    print("="*60)
    print(f"MinIO:        {MINIO_ENDPOINT} (Bucket: {MINIO_BUCKET})")
    print(f"Postgres:     {POSTGRES_HOST}:{POSTGRES_PORT} (DB: {POSTGRES_DB})")
    print(f"Features:     {CONTINUOUS_FEATURES}")
    print("="*60 + "\n")

if __name__ == "__main__":
    print_config()