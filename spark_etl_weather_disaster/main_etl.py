"""
MAIN ETL PIPELINE - LAMBDA ARCHITECTURE
Spark ETL Streaming: 
  1. Batch Layer: Kafka -> MinIO (Parquet) - Lưu trữ dài hạn
  2. Speed Layer: Kafka -> Redis (Key-Value) - Realtime Dashboard

CHẠY: python main_etl.py
"""

from pyspark.sql import SparkSession
import sys
import os

# Cấu hình môi trường
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import các modules
import kafka_config
import minio_config
# import redis_config (Không bắt buộc import ở đây nếu bên writer đã import)

from readers.real_data_reader import DataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from transformations.enrichment import DataEnricher

# --- MỚI: Import Redis Writer ---
from writers.redis_data_writer import RedisWriter 

def create_spark_session():
    """Khởi tạo Spark với hỗ trợ S3/MinIO"""
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherLambdaArchitecture") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", ",".join(packages))

    print("📦 Đang nạp cấu hình MinIO S3...")
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_etl_pipeline():
    print("\n" + "="*80)
    print("🚀 SPARK LAMBDA: KAFKA -> MINIO & REDIS")
    print("="*80)
    
    spark = create_spark_session()
    
    # 1. READ & TRANSFORM (Dùng chung cho cả 2 nhánh)
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    enricher = DataEnricher()
    
    print(f"\n🎧 Đang đọc topic '{kafka_config.KAFKA_TOPICS['weather']}'...")
    weather_df = reader.read_weather_data()
    
    print("⚙️  Đang xử lý dữ liệu...")
    weather_clean = cleaner.clean_weather_data(weather_df)
    weather_norm = normalizer.normalize_weather_data(weather_clean)
    weather_enriched = enricher.enrich_with_disaster_risk(weather_norm)
    
    # ==========================================
    # NHÁNH 1: BATCH LAYER -> MINIO (Lưu kho)
    # ==========================================
    output_minio = minio_config.get_minio_path("enriched", "weather", format="parquet")
    # Checkpoint riêng cho MinIO
    ckpt_minio = f"s3a://{minio_config.MINIO_BUCKET}/checkpoints/weather_minio"
    
    print(f"\n💾 Cấu hình MinIO (Batch Layer - 30s):")
    print(f"   👉 Path: {output_minio}")
    
    query_minio = weather_enriched.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_minio) \
        .option("checkpointLocation", ckpt_minio) \
        .trigger(processingTime="30 seconds") \
        .queryName("WriteToMinIO") \
        .start()

    # ==========================================
    # NHÁNH 2: SPEED LAYER -> REDIS (Realtime)
    # ==========================================
    # Khởi tạo Writer
    redis_writer = RedisWriter()
    # Checkpoint riêng cho Redis (QUAN TRỌNG: Không được trùng với MinIO)
    ckpt_redis = f"s3a://{minio_config.MINIO_BUCKET}/checkpoints/weather_redis"
    
    print(f"\n🔥 Cấu hình Redis (Speed Layer - 5s):")
    print(f"   👉 Checkpoint: {ckpt_redis}")
    
    query_redis = weather_enriched.writeStream \
        .outputMode("append") \
        .foreachBatch(redis_writer.write_stream_to_redis) \
        .option("checkpointLocation", ckpt_redis) \
        .trigger(processingTime="5 seconds") \
        .queryName("WriteToRedis") \
        .start()

    print("\n" + "="*80)
    print("✅ PIPELINE ĐA LUỒNG ĐANG CHẠY!")
    print("   1. MinIO: Ghi mỗi 30 giây (Lưu lịch sử)")
    print("   2. Redis: Ghi mỗi 5 giây (Cập nhật Dashboard)")
    print("👉 Nhấn Ctrl+C để dừng lại.")
    print("="*80)

    try:
        # Chờ BẤT KỲ query nào kết thúc (hoặc lỗi)
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng pipeline...")
        # Dừng từng query một cách an toàn
        if query_minio.isActive: query_minio.stop()
        if query_redis.isActive: query_redis.stop()
        spark.stop()
        print("✅ Đã dừng thành công.")

if __name__ == "__main__":
    try:
        run_etl_pipeline()
    except Exception as e:
        print(f"\n❌ LỖI: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)