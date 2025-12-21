from pyspark.sql import SparkSession
import sys
import os

# --- 1. SETUP MÔI TRƯỜNG ---
# Đảm bảo Spark sử dụng Python3 trong container
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

# Lấy đường dẫn thư mục gốc (/app)
# Script đang ở /app/job/main_etl.py -> parent là /app
current_dir = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(current_dir)

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# --- 2. IMPORT MODULES ---
try:
    from config import kafka_config
    from config import minio_config
except ImportError:
    # Dự phòng nếu PYTHONPATH đã trỏ trực tiếp vào config
    import kafka_config
    import minio_config

from readers.real_data_reader import DataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from writers.redis_data_writer import RedisWriter 
from writers.minio_writer import MinIOWriter

def create_spark_session():
    # Điều chỉnh version để khớp với Spark 3.3.x trong image v3
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
        .config("spark.jars.ivy", "/tmp/.ivy2") 

    # Nạp cấu hình MinIO (S3A)
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_etl_pipeline():
    print("\n" + "="*80)
    print("🚀 SPARK PIPELINE STARTING: KAFKA -> (MINIO + REDIS)")
    print("="*80)
    
    # 1. INIT
    spark = create_spark_session()
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    
    minio_writer = MinIOWriter()
    redis_writer = RedisWriter()
    
    # 2. READ
    weather_df = reader.read_weather_data()
    
    # 3. TRANSFORM
    weather_clean = cleaner.clean_weather_data(weather_df)
    weather_final = normalizer.normalize_weather_data(weather_clean)
    
    # 4. WRITE
    # Nhánh 1: MinIO (Data Lake)
    query_minio = minio_writer.write_stream(weather_final, folder="enriched")

    # Nhánh 2: Redis (Real-time Dashboard)
    query_redis = weather_final.writeStream \
        .outputMode("append") \
        .foreachBatch(redis_writer.write_stream_to_redis) \
        .option("checkpointLocation", "/tmp/checkpoints/weather_redis") \
        .trigger(processingTime="5 seconds") \
        .queryName("WriteToRedis") \
        .start()

    print("\n✅ STREAMING QUERIES STARTED. WAITING FOR DATA...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_etl_pipeline()