from pyspark.sql import SparkSession
import sys
import os

# --- 1. SETUP MÔI TRƯỜNG TUYỆT ĐỐI ---
# Thêm /app vào đầu sys.path để Python ưu tiên tìm các folder module gốc
if '/app' not in sys.path:
    sys.path.insert(0, '/app')

# Ép sử dụng Python3 trong container để tránh lỗi Exit Code 1
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

# --- 2. IMPORT MODULES ---
try:
    # SỬA TẠI ĐÂY: Import trực tiếp file, không dùng 'from config import'
    import kafka_config
    import minio_config
    import postgres_config
    import redis_config
    print("✅ Successfully imported config files from job folder")
except ImportError as e:
    print(f"❌ Failed to import configs: {e}")
    sys.exit(1)# Import các module chức năng từ root /app
from readers.real_data_reader import DataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from writers.redis_data_writer import RedisWriter 
from writers.minio_writer import MinIOWriter

def create_spark_session():
    # Sử dụng version 3.3.0 để khớp với Spark version trong image v3
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherLambdaArchitecture") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "800m") \
        .config("spark.executor.memory", "1g") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars.ivy", "/tmp/.ivy2") # Tránh lỗi quyền ghi cache

    # Nạp cấu hình MinIO
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_etl_pipeline():
    print("\n" + "="*80)
    print("🚀 SPARK PIPELINE STARTING: KAFKA -> (MINIO + REDIS)")
    print("="*80)
    
    spark = create_spark_session()
    
    # Khởi tạo các thành phần
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    minio_writer = MinIOWriter()
    redis_writer = RedisWriter()
    
    # Luồng xử lý
    weather_df = reader.read_weather_data()
    weather_clean = cleaner.clean_weather_data(weather_df)
    weather_final = normalizer.normalize_weather_data(weather_clean)
    
    # Ghi dữ liệu đa hướng (MinIO + Redis)
    query_minio = minio_writer.write_stream(weather_final, folder="enriched")

    query_redis = weather_final.writeStream \
        .outputMode("append") \
        .foreachBatch(redis_writer.write_stream_to_redis) \
        .option("checkpointLocation", "/tmp/checkpoints/weather_redis") \
        .trigger(processingTime="5 seconds") \
        .queryName("WriteToRedis") \
        .start()

    print("\n✅ STREAMING QUERIES STARTED. MONITORING DASHBOARD NOW...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_etl_pipeline()