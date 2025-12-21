import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from minio import Minio # Cần pip install minio

# --- SETUP ĐƯỜNG DẪN ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- CẤU HÌNH CỔNG MỚI (ĐỂ NÉ CỔNG CŨ BỊ LỖI) ---
# Map với lệnh port-forward bạn vừa chạy
os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6380"  # Cổng mới

os.environ["MINIO_ENDPOINT"] = "localhost:9090" # Cổng mới
os.environ["MINIO_ACCESS_KEY"] = "admin"        # User K8s
os.environ["MINIO_SECRET_KEY"] = "password123"  # Pass K8s

# Import modules
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from writers.redis_data_writer import RedisWriter
from config import minio_config

def auto_create_bucket():
    """Hàm tự động tạo Bucket, không cần vào Browser"""
    print("🛠️  Đang kiểm tra MinIO Bucket...")
    try:
        client = Minio(
            os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False
        )
        bucket_name = minio_config.MINIO_BUCKET
        
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"   ✅ Đã TỰ TẠO bucket: '{bucket_name}'")
        else:
            print(f"   ✅ Bucket '{bucket_name}' đã tồn tại.")
            
    except Exception as e:
        print(f"   ❌ Lỗi kết nối MinIO để tạo bucket: {e}")
        print("   👉 Bạn đã chạy 'kubectl port-forward ... 9090:9000' chưa?")
        sys.exit(1)

def create_local_spark():
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    return SparkSession.builder \
        .appName("LocalTest") \
        .master("local[1]") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.environ['MINIO_ENDPOINT']}") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def test_full_flow():
    # 0. TỰ TẠO BUCKET TRƯỚC
    auto_create_bucket()
    
    spark = create_local_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n" + "="*50)
    print("🧪 BẮT ĐẦU TEST LUỒNG DỮ LIỆU (AUTO BUCKET)")
    print("="*50)

    # 1. MOCK DATA
    data = [
        ("2024-01-01 12:00:00", "Hanoi", 25.567, 60.0, 1012.0, " rain ", 10.0, 5.0),
        ("2024-01-01 12:00:00", "BadCity", 1000.0, 60.0, 1012.0, "cloudy", 10.0, 5.0),
    ]
    schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("weather_desc", StringType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    print("1️⃣  Data Mock OK.")

    # 2. TEST CLEANER
    cleaner = DataCleaner()
    df_clean = cleaner.clean_weather_data(df)
    print(f"2️⃣  Cleaner OK (Rows: {df_clean.count()})")

    # 3. TEST NORMALIZER
    normalizer = DataNormalizer()
    df_final = normalizer.normalize_weather_data(df_clean)
    print(f"3️⃣  Normalizer OK")

    # 4. TEST REDIS
    print("4️⃣  Test Redis Writer (Port 6380)...")
    try:
        writer = RedisWriter()
        writer.write_stream_to_redis(df_final, 1)
        print("   ✅ Ghi Redis thành công!")
    except Exception as e:
        print(f"   ❌ Lỗi Redis: {e}")

    # 5. TEST MINIO
    print("5️⃣  Test MinIO Writer (Port 9090)...")
    try:
        output_path = f"s3a://{minio_config.MINIO_BUCKET}/test_output/weather"
        df_final.write.mode("overwrite").parquet(output_path)
        print(f"   ✅ Ghi MinIO thành công tại: {output_path}")
    except Exception as e:
        print(f"   ❌ Lỗi MinIO: {e}")

    spark.stop()

if __name__ == "__main__":
    test_full_flow()