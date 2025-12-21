from pyspark.sql import SparkSession
import sys
import os

# --- 1. SETUP MÔI TRƯỜNG ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# --- 2. IMPORT MODULES ---
try:
    from config import kafka_config
    from config import minio_config
except ImportError:
    import kafka_config
    import minio_config

from readers.real_data_reader import DataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
# Import 2 Writer chuyên dụng
from writers.redis_data_writer import RedisWriter 
from writers.minio_writer import MinIOWriter # <--- Class mới

def create_spark_session():
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherLambdaArchitecture") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars.ivy", "/tmp/.ivy2") 

    # Nạp config MinIO cho Spark
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_etl_pipeline():
    print("\n" + "="*80)
    print("🚀 SPARK PIPELINE: KAFKA -> CLEAN -> NORMALIZE -> (MINIO + REDIS)")
    print("="*80)
    
    # 1. INIT
    spark = create_spark_session()
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    
    # Khởi tạo 2 Writer
    minio_writer = MinIOWriter()
    redis_writer = RedisWriter()
    
    # 2. READ
    print(f"\n🎧 Reading Topic: {kafka_config.KAFKA_TOPICS['weather']}")
    weather_df = reader.read_weather_data()
    
    # 3. TRANSFORM
    weather_clean = cleaner.clean_weather_data(weather_df)
    weather_final = normalizer.normalize_weather_data(weather_clean)
    
    # 4. WRITE (Đa luồng)
    
    # Nhánh 1: Lưu trữ lâu dài (Data Lake) -> MinIO
    print(f"\n💾 Starting MinIO Stream...")
    query_minio = minio_writer.write_stream(weather_final, folder="enriched")

    # Nhánh 2: Hiển thị thời gian thực (Dashboard) -> Redis
    print(f"\n🔥 Starting Redis Stream...")
    query_redis = weather_final.writeStream \
        .outputMode("append") \
        .foreachBatch(redis_writer.write_stream_to_redis) \
        .option("checkpointLocation", f"s3a://{minio_config.MINIO_BUCKET}/checkpoints/weather_redis") \
        .trigger(processingTime="5 seconds") \
        .queryName("WriteToRedis") \
        .start()

    print("\n✅ PIPELINE IS RUNNING...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_etl_pipeline()