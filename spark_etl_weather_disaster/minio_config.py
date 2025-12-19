"""
MinIO Configuration
Cấu hình kết nối MinIO - S3-compatible object storage

HƯỚNG DẪN:
1. Development (Local): Dùng MinIO local server (localhost:9000)
2. Production: Thay đổi MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

SETUP MinIO LOCAL (Docker):
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  quay.io/minio/minio server /data --console-address ":9001"

Sau đó truy cập: http://localhost:9001 để quản lý buckets
"""

# ===========================
# MINIO CONFIGURATION
# ===========================

# MinIO Server Endpoint (không bao gồm http://)
# Local: localhost:9000
# Production: Thay bằng endpoint thật (ví dụ: minio.yourcompany.com:9000)
MINIO_ENDPOINT = "localhost:9000"

# MinIO Access Credentials
# Local default: minioadmin/minioadmin
# Production: Thay bằng credentials thật
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Sử dụng HTTPS hay không (Local thường dùng False)
MINIO_SECURE = False  # True cho production với SSL

# ===========================
# BUCKET CONFIGURATION
# ===========================

# Bucket chính để lưu dữ liệu weather
MINIO_BUCKET = "weather-data"

# Folder structure trong bucket
MINIO_FOLDERS = {
    "cleaned": "cleaned",        # Dữ liệu đã cleaned
    "enriched": "enriched",      # Dữ liệu đã enriched/integrated
    "raw": "raw",                # (Optional) Dữ liệu thô
    "archive": "archive"         # (Optional) Dữ liệu archive
}

# ===========================
# SPARK S3 CONFIGURATION
# ===========================
# Các config này sẽ được thêm vào SparkSession

SPARK_S3_CONFIG = {
    # S3A filesystem implementation
    "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}" if not MINIO_SECURE else f"https://{MINIO_ENDPOINT}",
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",  # Required for MinIO
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    
    # Connection settings
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true" if MINIO_SECURE else "false",
    
    # Performance tuning (optional)
    "spark.hadoop.fs.s3a.block.size": "128M",
    "spark.hadoop.fs.s3a.buffer.dir": "/tmp",
}

# ===========================
# HELPER FUNCTIONS
# ===========================

def get_minio_path(folder: str, dataset_name: str, format: str = "parquet") -> str:
    """
    Tạo đường dẫn S3 cho MinIO
    
    Args:
        folder: Tên folder (cleaned, enriched, etc.)
        dataset_name: Tên dataset (weather, 311_requests, etc.)
        format: Format file (parquet, json, csv)
    
    Returns:
        str: S3 path (ví dụ: s3a://weather-data/cleaned/weather)
    """
    folder_path = MINIO_FOLDERS.get(folder, folder)
    return f"s3a://{MINIO_BUCKET}/{folder_path}/{dataset_name}"


def print_config():
    """In ra cấu hình hiện tại (để debug)"""
    print("\n" + "="*80)
    print("📦 MINIO CONFIGURATION")
    print("="*80)
    print(f"Endpoint:     {MINIO_ENDPOINT}")
    print(f"Access Key:   {MINIO_ACCESS_KEY[:4]}****")
    print(f"Bucket:       {MINIO_BUCKET}")
    print(f"Secure (SSL): {MINIO_SECURE}")
    print(f"Folders:      {list(MINIO_FOLDERS.keys())}")
    print("="*80 + "\n")


if __name__ == "__main__":
    # Test configuration
    print_config()
    
    # Test path generation
    print("Example paths:")
    print(f"  Cleaned weather: {get_minio_path('cleaned', 'weather')}")
    print(f"  Enriched data:   {get_minio_path('enriched', 'integrated')}")


def validate_config(test_connection: bool = False):
    """
    Validate MinIO configuration
    
    Args:
        test_connection: If True, test actual connection to MinIO server
        
    Returns:
        bool: True if valid, raises ValueError if invalid
    """
    print("\n🔍 Validating MinIO Configuration...")
    
    # Check required fields
    if not MINIO_ENDPOINT:
        raise ValueError("MINIO_ENDPOINT is not set!")
    
    if not MINIO_ACCESS_KEY:
        raise ValueError("MINIO_ACCESS_KEY is not set!")
    
    if not MINIO_SECRET_KEY:
        raise ValueError("MINIO_SECRET_KEY is not set!")
    
    if not MINIO_BUCKET:
        raise ValueError("MINIO_BUCKET is not set!")
    
    # Warn if using defaults
    if "localhost" in MINIO_ENDPOINT.lower():
        print("   ⚠️  WARNING: Using localhost MinIO server")
        print("      Make sure MinIO is running locally or update config for production")
    
    if MINIO_ACCESS_KEY == "minioadmin":
        print("   ⚠️  WARNING: Using default MinIO credentials (minioadmin)")
        print("      Change these for production!")
    
    print("   ✅ Configuration validation passed!")
    
    # Optional: Test actual connection
    if test_connection:
        try:
            from connection_utils import validate_minio_connection
            if not validate_minio_connection(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE):
                print("   ⚠️  Connection test failed but continuing anyway")
        except ImportError:
            print("   💡 connection_utils not found, skipping connection test")
    
    print_config()
    return True
