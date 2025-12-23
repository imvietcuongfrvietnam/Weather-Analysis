"""
Weather Forecasting ML - Configuration
Cấu hình cho hệ thống dự đoán thời tiết
Updated: Removed precipitation_mm, Added wind_direction
"""

import os

# ========================================
# 1. MINIO CONFIGURATION
# ========================================
# Endpoint nội bộ trong Kubernetes
MINIO_HOST_FQDN = "weather-minio.default.svc.cluster.local"
MINIO_PORT = "9000"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", f"{MINIO_HOST_FQDN}:{MINIO_PORT}")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")

# Paths
MINIO_INPUT_PATH = f"s3a://{MINIO_BUCKET}/enriched/weather/"
MINIO_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/ml/forecasts/"
MINIO_MODEL_PATH = f"s3a://{MINIO_BUCKET}/ml/models/"

SPARK_S3_CONFIG = {
    "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}",
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1"
}

# ========================================
# 2. POSTGRESQL CONFIGURATION
# ========================================
# Cập nhật thông tin khớp với PostgresConnector đã chạy thành công
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "weather-postgresql.default.svc.cluster.local")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")           # Sửa thành weather_db
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")     # Sửa thành weather_user
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass") # Sửa thành weather_pass
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

POSTGRES_WRITE_MODE = "append"

# ========================================
# 3. ML FEATURE CONFIGURATION
# ========================================
# Cột input có sẵn: datetime, City, temperature, humidity, pressure, weather_desc, wind_direction, wind_speed

# 1. Các biến liên tục (Số) - Đã xóa precipitation, thêm wind_direction
CONTINUOUS_FEATURES = [
    'temperature',      
    'humidity',         
    'pressure',         
    'wind_speed',       
    'wind_direction'    # <--- Mới thêm vào
]

# 2. Các biến phân loại (Chữ)
CATEGORICAL_FEATURES = ['weather_desc']

# Tổng hợp (datetime và City được dùng làm Index/Filter chứ không train trực tiếp)
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
# Đã xóa precipitation_mm, thêm wind_direction
MODEL_SELECTION = {
    'temperature': 'GBT',
    'humidity': 'GBT',
    'pressure': 'GBT',
    'wind_speed': 'RandomForest',
    'wind_direction': 'RandomForest', # <--- Mới thêm
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
MIN_TRAINING_RECORDS = 50
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