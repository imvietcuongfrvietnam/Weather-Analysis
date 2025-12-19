"""
Weather Forecasting ML - Configuration
Cấu hình cho hệ thống dự đoán thời tiết 7 ngày
"""

import os
import sys

# Add ETL project to path to reuse minio_config
ETL_PROJECT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "spark_etl_weather_disaster")
if ETL_PROJECT_PATH not in sys.path:
    sys.path.append(ETL_PROJECT_PATH)

# ========================================
# MINIO CONFIGURATION
# ========================================
# Reuse from ETL project
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False
MINIO_BUCKET = "weather-data"

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
}

# ========================================
# POSTGRESQL CONFIGURATION
# ========================================
# Lấy từ biến môi trường, dễ dàng thay đổi khi deploy
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_forecast")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")

# JDBC URL for Spark
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# JDBC Driver
POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"

# Connection properties
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": POSTGRES_JDBC_DRIVER
}

# Write mode: "append", "overwrite", "ignore", "error"
POSTGRES_WRITE_MODE = os.getenv("POSTGRES_WRITE_MODE", "append")

# ========================================
# FORECASTING PARAMETERS
# ========================================
FORECAST_HORIZON_HOURS = 168  # 7 days
FORECAST_INTERVALS = [1, 6, 12, 24, 48, 72, 120, 168]  # Hours to evaluate

# ========================================
# FEATURE ENGINEERING
# ========================================
# Lag features (past values to use as predictors)
LAG_HOURS = [1, 3, 6, 12, 24, 48]

# Rolling window sizes (for calculating statistics)
ROLLING_WINDOWS = [6, 12, 24]

# Features to predict (target variables)
CONTINUOUS_FEATURES = [
    'temp_celsius',
    'humidity_pct', 
    'pressure_hpa',
    'wind_speed_kmh',
    'precipitation_mm'
]

CATEGORICAL_FEATURES = [
    'weather_condition'  # clear, cloudy, rain, snow, storm
]

# All target features
ALL_TARGET_FEATURES = CONTINUOUS_FEATURES + CATEGORICAL_FEATURES

# Base features from ETL (available in data)
BASE_FEATURES = [
    'datetime',
    'city',
    'temperature',
    'temp_celsius',
    'temp_fahrenheit',
    'humidity',
    'humidity_pct',
    'pressure',
    'pressure_hpa',
    'wind_speed',
    'wind_speed_kmh',
    'wind_direction',
    'precipitation_mm',
    'rain_1h',
    'snow_1h',
    'weather_description',
    'weather_condition',
    'weather_severity',
    'clouds_all'
]

# ========================================
# MODEL PARAMETERS
# ========================================
# Training/Testing split
TRAIN_TEST_SPLIT = 0.8

# Random seed for reproducibility
RANDOM_SEED = 42

# Model hyperparameters
GBT_PARAMS = {
    'maxIter': 100,
    'maxDepth': 6,
    'stepSize': 0.1,
    'subsamplingRate': 0.8
}

RF_REGRESSION_PARAMS = {
    'numTrees': 100,
    'maxDepth': 8,
    'minInstancesPerNode': 1,
    'subsamplingRate': 0.8
}

RF_CLASSIFICATION_PARAMS = {
    'numTrees': 150,
    'maxDepth': 10,
    'minInstancesPerNode': 5
}

# ========================================
# MODEL SELECTION PER FEATURE
# ========================================
# Which model to use for each feature
MODEL_SELECTION = {
    'temp_celsius': 'GBT',          # Gradient Boosted Trees
    'humidity_pct': 'GBT',
    'pressure_hpa': 'GBT',
    'wind_speed_kmh': 'RandomForest',
    'precipitation_mm': 'RandomForest',
    'weather_condition': 'RandomForestClassifier'
}

# ========================================
# OUTPUT CONFIGURATION
# ========================================
# Local output directory
LOCAL_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "forecasts")
LOCAL_MODEL_DIR = os.path.join(os.path.dirname(__file__), "saved_models")
LOCAL_PLOTS_DIR = os.path.join(LOCAL_OUTPUT_DIR, "plots")

# Create directories if they don't exist
for directory in [LOCAL_OUTPUT_DIR, LOCAL_MODEL_DIR, LOCAL_PLOTS_DIR]:
    os.makedirs(directory, exist_ok=True)

# ========================================
# DATA QUALITY
# ========================================
# Minimum number of records required for training
MIN_TRAINING_RECORDS = 100

# Minimum number of days of historical data preferred
MIN_DAYS_HISTORY = 14  # 2 weeks minimum, 30+ days preferred

# Maximum allowed missing value percentage
MAX_MISSING_PCT = 0.1  # 10%

# ========================================
# LOGGING
# ========================================
LOG_LEVEL = "INFO"

def print_config():
    """Print current configuration"""
    print("\n" + "="*80)
    print("⚙️  WEATHER FORECASTING ML - CONFIGURATION")
    print("="*80)
    print(f"MinIO Endpoint:       {MINIO_ENDPOINT}")
    print(f"Input Path:           {MINIO_INPUT_PATH}")
    print(f"PostgreSQL:           {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"  Table:              {POSTGRES_TABLE}")
    print(f"  Write Mode:         {POSTGRES_WRITE_MODE}")
    print(f"Forecast Horizon:     {FORECAST_HORIZON_HOURS} hours (7 days)")
    print(f"Target Features:      {len(ALL_TARGET_FEATURES)}")
    print(f"  - Continuous:       {CONTINUOUS_FEATURES}")
    print(f"  - Categorical:      {CATEGORICAL_FEATURES}")
    print(f"Lag Features:         {LAG_HOURS}")
    print(f"Rolling Windows:      {ROLLING_WINDOWS}")
    print(f"Train/Test Split:     {int(TRAIN_TEST_SPLIT*100)}% / {int((1-TRAIN_TEST_SPLIT)*100)}%")
    print(f"Output Directory:     {LOCAL_OUTPUT_DIR}")
    print("="*80 + "\n")

if __name__ == "__main__":
    print_config()
