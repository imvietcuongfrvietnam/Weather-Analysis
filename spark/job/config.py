"""
Weather Forecasting ML - Configuration
Cấu hình cho hệ thống dự đoán thời tiết 7 ngày
"""

import os
import sys

# ========================================
# KUBERNETES COMPATIBILITY SETUP
# ========================================
# Khi chạy trong K8s, ta ưu tiên lấy cấu hình từ Biến môi trường (Environment Variables)
# Các giá trị default bên dưới được sửa để khớp với tên Service trong K8s

# ========================================
# MINIO CONFIGURATION
# ========================================
# SỬA: Default host đổi thành tên Service trong K8s để tránh lỗi kết nối
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "weather-minio:9000")
# Lưu ý: Nếu chạy Local (ngoài K8s), bạn cần set env var: export MINIO_ENDPOINT=localhost:9000

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")       # Khớp với YAML
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123") # Khớp với YAML
MINIO_SECURE = False
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")

# MinIO Paths
MINIO_INPUT_PATH = f"s3a://{MINIO_BUCKET}/enriched/weather/"
MINIO_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/ml/forecasts/"
MINIO_MODEL_PATH = f"s3a://{MINIO_BUCKET}/ml/models/"

# Spark S3A Configuration
SPARK_S3A_CONFIG = {
    "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}" if not MINIO_SECURE else f"https://{MINIO_ENDPOINT}",
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true" if MINIO_SECURE else "false",
    # Tối ưu cho MinIO
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.committer.name": "directory",
    "spark.hadoop.fs.s3a.committer.staging.conflict-mode": "replace"
}

# ========================================
# POSTGRESQL CONFIGURATION
# ========================================
# SỬA: Default host đổi thành tên Service trong K8s
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "weather-postgresql") 
# Lưu ý: Nếu chạy Local, set env var: export POSTGRES_HOST=localhost

POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")             # Khớp với YAML
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")       # Khớp với YAML
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")

# JDBC URL for Spark
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": POSTGRES_JDBC_DRIVER
}

POSTGRES_WRITE_MODE = os.getenv("POSTGRES_WRITE_MODE", "append")

# ========================================
# CÁC PHẦN LOGIC ML GIỮ NGUYÊN
# ========================================
# ... (Giữ nguyên phần Forecasting Parameters, Feature Engineering, Model Parameters như cũ) ...
FORECAST_HORIZON_HOURS = 168
FORECAST_INTERVALS = [1, 6, 12, 24, 48, 72, 120, 168]

LAG_HOURS = [1, 3, 6, 12, 24, 48]
ROLLING_WINDOWS = [6, 12, 24]

CONTINUOUS_FEATURES = ['temp_celsius', 'humidity_pct', 'pressure_hpa', 'wind_speed_kmh', 'precipitation_mm']
CATEGORICAL_FEATURES = ['weather_condition']
ALL_TARGET_FEATURES = CONTINUOUS_FEATURES + CATEGORICAL_FEATURES

BASE_FEATURES = [
    'datetime', 'city', 'temperature', 'temp_celsius', 'temp_fahrenheit',
    'humidity', 'humidity_pct', 'pressure', 'pressure_hpa',
    'wind_speed', 'wind_speed_kmh', 'wind_direction',
    'precipitation_mm', 'rain_1h', 'snow_1h',
    'weather_description', 'weather_condition', 'weather_severity', 'clouds_all'
]

TRAIN_TEST_SPLIT = 0.8
RANDOM_SEED = 42

GBT_PARAMS = {'maxIter': 100, 'maxDepth': 6, 'stepSize': 0.1, 'subsamplingRate': 0.8}
RF_REGRESSION_PARAMS = {'numTrees': 100, 'maxDepth': 8, 'minInstancesPerNode': 1, 'subsamplingRate': 0.8}
RF_CLASSIFICATION_PARAMS = {'numTrees': 150, 'maxDepth': 10, 'minInstancesPerNode': 5}

MODEL_SELECTION = {
    'temp_celsius': 'GBT',
    'humidity_pct': 'GBT',
    'pressure_hpa': 'GBT',
    'wind_speed_kmh': 'RandomForest',
    'precipitation_mm': 'RandomForest',
    'weather_condition': 'RandomForestClassifier'
}

# ========================================
# OUTPUT CONFIGURATION
# ========================================
# Lưu ý: Trong Pod K8s, các folder này là tạm thời (Ephemeral)
LOCAL_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "forecasts")
LOCAL_MODEL_DIR = os.path.join(os.path.dirname(__file__), "saved_models")
LOCAL_PLOTS_DIR = os.path.join(LOCAL_OUTPUT_DIR, "plots")

for directory in [LOCAL_OUTPUT_DIR, LOCAL_MODEL_DIR, LOCAL_PLOTS_DIR]:
    os.makedirs(directory, exist_ok=True)

# ========================================
# DATA QUALITY & LOGGING
# ========================================
MIN_TRAINING_RECORDS = 100
MIN_DAYS_HISTORY = 14
MAX_MISSING_PCT = 0.1
LOG_LEVEL = "INFO"

def print_config():
    """Print current configuration"""
    print("\n" + "="*80)
    print("⚙️  WEATHER FORECASTING ML - K8S CONFIGURATION")
    print("="*80)
    print(f"MinIO Endpoint:       {MINIO_ENDPOINT}")
    print(f"Input Path:           {MINIO_INPUT_PATH}")
    print(f"PostgreSQL:           {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"  Table:              {POSTGRES_TABLE}")
    print(f"Forecast Horizon:     {FORECAST_HORIZON_HOURS} hours")
    print(f"Local Output:         {LOCAL_OUTPUT_DIR} (Temporary in Pod)")
    print("="*80 + "\n")

if __name__ == "__main__":
    print_config()