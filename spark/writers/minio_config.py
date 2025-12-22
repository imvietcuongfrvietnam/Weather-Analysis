import os

# Lấy từ env (do Airflow cấp) hoặc mặc định theo file minio.yaml
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

MINIO_BUCKET = "weather-data"

# Gọi xuyên Namespace từ 'airflow' sang 'default'
MINIO_ENDPOINT = "http://weather-minio.default.svc.cluster.local:9000"

SPARK_S3_CONFIG = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    # Thêm region để tránh lỗi DNS khi AWS SDK cố tìm region thực
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1" 
}