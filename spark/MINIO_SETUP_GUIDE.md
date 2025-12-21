# MinIO Setup and Testing Guide

## 📦 MinIO Nhanh

### Cài đặt MinIO Dependencies

```bash
pip install minio pyarrow
```

**Lưu ý:** Để sử dụng Spark với MinIO, cần thêm Hadoop AWS connector khi chạy Spark:
```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1 main_etl.py
```

---

## 🚀 Cách 1: Setup MinIO Local với Docker (Khuyến nghị)

### Bước 1: Chạy MinIO Server

```bash
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  --name minio-server \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  -v ~/minio-data:/data \
  quay.io/minio/minio server /data --console-address ":9001"
```

### Bước 2: Truy cập MinIO Console

Mở browser: http://localhost:9001

- **Username:** minioadmin
- **Password:** minioadmin

### Bước 3: Tạo Bucket

Trong console, tạo bucket tên `weather-data`

---

## 🖥️ Cách 2: Chạy MinIO Standalone (Không dùng Docker)

### Download MinIO

**Linux:**
```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server /mnt/data --console-address ":9001"
```

**Windows:**
```powershell
# Download từ: https://dl.min.io/server/minio/release/windows-amd64/minio.exe
.\minio.exe server C:\minio-data --console-address ":9001"
```

**MacOS:**
```bash
brew install minio/stable/minio
minio server /Users/username/minio-data --console-address ":9001"
```

---

## ⚙️ Cấu hình MinIO cho Project

### File: `minio_config.py`

Đã được tạo sẵn với cấu hình mặc định cho local testing:

```python
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False
MINIO_BUCKET = "weather-data"
```

**Khi deploy lên server thật:**

1. Mở file `minio_config.py`
2. Thay đổi các giá trị:
   ```python
   MINIO_ENDPOINT = "your-minio-server.com:9000"
   MINIO_ACCESS_KEY = "your-access-key"
   MINIO_SECRET_KEY = "your-secret-key"
   MINIO_SECURE = True  # Nếu dùng HTTPS
   ```

---

## 🧪 Testing

### Test 1: Test MinIO Configuration

```bash
cd spark_etl_weather_disaster
python minio_config.py
```

**Kết quả mong đợi:**
```
================================================================================
📦 MINIO CONFIGURATION
================================================================================
Endpoint:     localhost:9000
Access Key:   mini****
Bucket:       weather-data
Secure (SSL): False
Folders:      ['cleaned', 'enriched', 'raw', 'archive']
================================================================================
```

### Test 2: Chạy ETL với JSON Mode (Không cần MinIO)

```bash
python main_etl.py
```

Dữ liệu sẽ được ghi vào `./output/` folder (JSON files).

### Test 3: Chạy ETL với MinIO Mode

**Bước 1:** Đảm bảo MinIO server đang chạy

**Bước 2:** Sửa file `main_etl.py` (dòng 137):

```python
# Thay đổi từ:
data_writer = DataWriter(output_type="json")

# Sang:
data_writer = DataWriter(output_type="minio")
```

**Bước 3:** Chạy ETL

```bash
python main_etl.py
```

**Bước 4:** Kiểm tra dữ liệu trong MinIO

Truy cập http://localhost:9001, vào bucket `weather-data`:

```
weather-data/
├── cleaned/
│   ├── weather/data.parquet
│   ├── 311_requests/data.parquet
│   ├── taxi_trips/data.parquet
│   └── collisions/data.parquet
└── enriched/
    └── integrated/data.parquet
```

---

## 🔍 Xác minh Dữ liệu

### Đọc lại dữ liệu từ MinIO bằng Python

```python
from minio import Minio
import pandas as pd
import io

# Connect to MinIO
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Download file
response = client.get_object("weather-data", "enriched/integrated/data.parquet")
data = response.read()

# Read parquet
df = pd.read_parquet(io.BytesIO(data))
print(df.head())
print(f"Total records: {len(df)}")
```

### Đọc lại dữ liệu từ MinIO bằng Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.parquet("s3a://weather-data/enriched/integrated")
df.show(10)
```

---

## 🐛 Troubleshooting

### Lỗi: "Could not connect to MinIO"

**Nguyên nhân:** MinIO server không chạy hoặc endpoint sai

**Giải pháp:**
1. Check MinIO server: `docker ps` (nếu dùng Docker)
2. Verify endpoint trong `minio_config.py`
3. Test connection: `telnet localhost 9000`

### Lỗi: "Bucket does not exist"

**Nguyên nhân:** Bucket chưa được tạo

**Giải pháp:**
1. Truy cập MinIO console: http://localhost:9001
2. Tạo bucket tên `weather-data`
3. Hoặc code sẽ tự động tạo bucket khi chạy lần đầu

### Lỗi: "Access Denied"

**Nguyên nhân:** Credentials sai

**Giải pháp:**
1. Check `MINIO_ACCESS_KEY` và `MINIO_SECRET_KEY` trong `minio_config.py`
2. Verify trong MinIO console: Access Keys section

### ETL chạy nhưng không thấy file trong MinIO

**Nguyên nhân:** DataWriter vẫn dùng mode "json"

**Giải pháp:**
1. Sửa `main_etl.py` line 137: `output_type="minio"`
2. Chạy lại ETL

---

## 📝 Next Steps

1. ✅ **Đã hoàn thành:**
   - MinIO configuration file
   - MinIO writer implementation
   - S3 support trong Spark session

2. **Để làm tiếp:**
   - Setup MinIO server (local hoặc production)
   - Test ETL với MinIO
   - Implement Kafka streaming reader
   - Add monitoring và error handling
