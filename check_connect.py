from pyspark.sql import SparkSession
import time

# --- CẤU HÌNH (Đã sửa theo tên Service của bạn) ---
# Tên service MinIO trong K8s: weather-minio
minio_endpoint = "http://weather-minio:9000"
# User/Pass mặc định của Bitnami/Helm chart thường là minio/minio123
# Nếu bạn deploy thủ công thì dùng user/pass bạn đã set.
# Sửa lại user/pass chuẩn
minio_access_key = "admin"
minio_secret_key = "password123"
bucket_name = "test-spark"

# Tên service Kafka trong K8s: weather-kafka
kafka_bootstrap_servers = "weather-kafka:9092"
topic_name = "weather-data"

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("CheckConnection") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("\n>>> SPARK SESSION STARTED")

# --- TEST 1: KẾT NỐI MINIO ---
try:
    print(f"\n--- [1] Đang thử ghi vào MinIO ({minio_endpoint})... ---")
    data = [("Ket noi", "Thanh cong"), ("MinIO", "OK")]
    df = spark.createDataFrame(data, ["Key", "Value"])
    
    # Ghi file CSV
    path = f"s3a://{bucket_name}/check_connection"
    df.write.mode("overwrite").csv(path)
    print(f">>> SUCCESS: Đã ghi dữ liệu thành công vào {path}")
except Exception as e:
    print(f">>> ERROR: Lỗi kết nối MinIO: {str(e)}")

# --- TEST 2: KẾT NỐI KAFKA ---
try:
    print(f"\n--- [2] Đang thử kết nối Kafka ({kafka_bootstrap_servers})... ---")
    # Đọc thử stream (không start, chỉ load definition)
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_name) \
        .load()
    
    print(">>> SUCCESS: Đã kết nối được tới Kafka Broker!")
    df_kafka.printSchema()
except Exception as e:
    print(f">>> ERROR: Lỗi kết nối Kafka: {str(e)}")

spark.stop()