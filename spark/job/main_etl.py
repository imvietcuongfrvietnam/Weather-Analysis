from pyspark.sql import SparkSession
import sys
import os

# --- 1. SETUP MÔI TRƯỜNG PYTHON ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- 2. FIX ĐƯỜNG DẪN (PATH) ---
# Lấy đường dẫn gốc của project (Folder 'spark')
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# --- 3. IMPORT CONFIGS (SỬA LẠI CHO CHUẨN) ---
# Vì file nằm trong folder 'config', ta phải import từ package 'config'
try:
    from config import kafka_config
    from config import minio_config
except ImportError:
    # Fallback: Nếu bạn lỡ để file config ngay ngoài root
    import kafka_config
    import minio_config

# --- 4. IMPORT MODULES XỬ LÝ ---
from readers.real_data_reader import DataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from transformations.enrichment import DataEnricher
from writers.redis_data_writer import RedisWriter 

def create_spark_session():
    """Khởi tạo Spark Session tối ưu cho Kubernetes"""
    
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" # Thêm Kafka Package nếu chưa có trong image
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherLambdaArchitecture") \
        # Lưu ý: 'local[*]' nghĩa là chạy client mode bên trong Pod. 
        # Với Deployment 1 Pod như hiện tại thì OK.
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars.ivy", "/tmp/.ivy2") 

    print("📦 Đang nạp cấu hình MinIO S3...")
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_etl_pipeline():
    print("\n" + "="*80)
    print("🚀 SPARK LAMBDA ON K8S: KAFKA -> MINIO & REDIS")
    print("="*80)
    
    # 1. Khởi tạo Spark
    try:
        spark = create_spark_session()
        print("✅ Spark Session đã khởi tạo thành công!")
    except Exception as e:
        print(f"❌ Lỗi khởi tạo Spark: {e}")
        sys.exit(1)
    
    # 2. READ
    # Đảm bảo Reader của bạn dùng đúng biến KAFKA_BOOTSTRAP_SERVERS từ file config
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    enricher = DataEnricher()
    
    print(f"\n🎧 Đang đọc topic '{kafka_config.KAFKA_TOPICS['weather']}'...")
    weather_df = reader.read_weather_data()
    
    # 3. TRANSFORM
    print("⚙️  Đang xử lý dữ liệu...")
    weather_clean = cleaner.clean_weather_data(weather_df)
    weather_norm = normalizer.normalize_weather_data(weather_clean)
    weather_enriched = enricher.enrich_with_disaster_risk(weather_norm)
    
    # ==========================================
    # NHÁNH 1: BATCH LAYER -> MINIO
    # ==========================================
    output_minio = minio_config.get_minio_path("enriched", "weather", format="parquet")
    ckpt_minio = f"s3a://{minio_config.MINIO_BUCKET}/checkpoints/weather_minio"
    
    print(f"\n💾 MinIO Writer (Batch): {output_minio}")
    
    # Thêm trigger availableNow=False để đảm bảo chạy streaming liên tục
    query_minio = weather_enriched.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_minio) \
        .option("checkpointLocation", ckpt_minio) \
        .trigger(processingTime="30 seconds") \
        .queryName("WriteToMinIO") \
        .start()

    # ==========================================
    # NHÁNH 2: SPEED LAYER -> REDIS
    # ==========================================
    redis_writer = RedisWriter()
    ckpt_redis = f"s3a://{minio_config.MINIO_BUCKET}/checkpoints/weather_redis"
    
    print(f"\n🔥 Redis Writer (Realtime): weather-redis")
    
    query_redis = weather_enriched.writeStream \
        .outputMode("append") \
        .foreachBatch(redis_writer.write_stream_to_redis) \
        .option("checkpointLocation", ckpt_redis) \
        .trigger(processingTime="5 seconds") \
        .queryName("WriteToRedis") \
        .start()

    print("\n" + "="*80)
    print("✅ PIPELINE ĐANG CHẠY TRONG KUBERNETES POD")
    print("="*80)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng pipeline...")
        spark.stop()

if __name__ == "__main__":
    run_etl_pipeline()