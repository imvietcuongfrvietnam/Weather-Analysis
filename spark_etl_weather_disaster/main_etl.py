"""
MAIN ETL PIPELINE
Spark ETL Streaming: Kafka -> Spark -> MinIO (S3)

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
import minio_config  # <--- QUAN TRỌNG: Phải có cái này mới kết nối được
from readers.real_data_reader import DataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from transformations.enrichment import DataEnricher

def create_spark_session():
    """Khởi tạo Spark với hỗ trợ S3/MinIO"""
    
    # 1. Định nghĩa thư viện AWS để Spark nói chuyện được với MinIO
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherStreamingToMinIO") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", ",".join(packages)) # <--- Tải thư viện AWS

    # 2. Nạp cấu hình MinIO từ file config
    print("📦 Đang nạp cấu hình MinIO S3...")
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_etl_pipeline():
    print("\n" + "="*80)
    print("🚀 SPARK STREAMING: KAFKA -> MINIO (DATA LAKE)")
    print("="*80)
    
    # 1. KHỞI TẠO
    spark = create_spark_session()
    
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    enricher = DataEnricher()
    
    # 2. ĐỌC DỮ LIỆU TỪ KAFKA
    print(f"\n🎧 Đang đọc topic '{kafka_config.KAFKA_TOPICS['weather']}'...")
    weather_df = reader.read_weather_data()
    
    # 3. XỬ LÝ DỮ LIỆU
    print("⚙️  Đang xử lý dữ liệu...")
    weather_clean = cleaner.clean_weather_data(weather_df)
    weather_norm = normalizer.normalize_weather_data(weather_clean)
    weather_enriched = enricher.enrich_with_disaster_risk(weather_norm)
    
    # 4. CẤU HÌNH ĐƯỜNG DẪN MINIO
    # Lấy đường dẫn từ file minio_config
    output_path = minio_config.get_minio_path("enriched", "weather", format="parquet")
    
    # Checkpoint là BẮT BUỘC khi ghi xuống MinIO
    checkpoint_path = f"s3a://{minio_config.MINIO_BUCKET}/checkpoints/weather"
    
    print(f"\n💾 Cấu hình lưu trữ:")
    print(f"   👉 Output Path:     {output_path}")
    print(f"   👉 Checkpoint Path: {checkpoint_path}")
    
    # 5. GHI DỮ LIỆU XUỐNG MINIO
    # Chuyển format từ console sang parquet
    query = weather_enriched.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="30 seconds") \
        .queryName("WeatherToMinIO") \
        .start()

    print("\n" + "="*80)
    print("✅ PIPELINE ĐANG CHẠY NGẦM!")
    print("👉 Dữ liệu đang được đẩy vào MinIO mỗi 30 giây.")
    print("👉 Kiểm tra tại: http://localhost:9001/browser/weather-data")
    print("👉 Nhấn Ctrl+C để dừng lại.")
    print("="*80)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng pipeline...")
        query.stop()
        spark.stop()
        print("✅ Đã dừng thành công.")

if __name__ == "__main__":
    try:
        run_etl_pipeline()
    except Exception as e:
        print(f"\n❌ LỖI: {e}")
        if "ClassNotFoundException" in str(e) or "hadoop-aws" in str(e):
            print("\n💡 GỢI Ý: Lỗi này do thiếu thư viện hadoop-aws. Hãy kiểm tra kết nối internet để Spark tải về.")
        import traceback
        traceback.print_exc()
        sys.exit(1)