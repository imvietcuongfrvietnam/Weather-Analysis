import os

# Kubernetes Service: weather-minio:9000
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")

# Lấy user/pass từ biến môi trường để khớp với manual_deploy.yaml
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MINIO_SECURE = False
MINIO_BUCKET = "weather-data"
MINIO_FOLDERS = {
    "cleaned": "cleaned",
    "enriched": "enriched",
    "raw": "raw",
    "archive": "archive"
}