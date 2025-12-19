# Hướng Dẫn: Dự Đoán Thời Tiết 7 Ngày Tới bằng LSTM

## 📋 Tổng Quan

Pipeline này đọc dữ liệu thời tiết từ MinIO, sử dụng mô hình LSTM (Long Short-Term Memory) để dự đoán các giá trị thời tiết trong 7 ngày tới, và ghi kết quả vào PostgreSQL.

## 🏗️ Kiến Trúc Pipeline

```
MinIO (Parquet) → Spark → LSTM Model → PostgreSQL → Grafana
```

1. **MinIO**: Lưu trữ dữ liệu thời tiết đã được xử lý (từ ETL pipeline)
2. **Spark**: Đọc và xử lý dữ liệu từ MinIO
3. **LSTM Model**: Mô hình deep learning để dự đoán time series
4. **PostgreSQL**: Lưu trữ kết quả dự đoán
5. **Grafana**: Visualize dữ liệu (tùy chọn)

## 📦 Yêu Cầu Hệ Thống

### 1. Phần Mềm Cần Cài Đặt

```bash
# Python dependencies
pip install -r ../spark_etl_weather_disaster/requirements.txt

# Hoặc cài đặt từng package:
pip install pyspark>=3.3.0
pip install pandas>=1.5.0
pip install numpy>=1.23.0
pip install tensorflow>=2.10.0
pip install scikit-learn>=1.0.0
pip install psycopg2-binary>=2.9.0
pip install minio
pip install pyarrow>=10.0.0
```

### 2. Spark Packages

Khi chạy với `spark-submit`, cần thêm các packages:

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0 \
  spark_lstm_forecast.py
```

### 3. Services Cần Chạy

- **MinIO**: Server lưu trữ dữ liệu (localhost:9000)
- **PostgreSQL**: Database server (localhost:5432)

## 🚀 Hướng Dẫn Cài Đặt và Chạy

### Bước 1: Cài Đặt PostgreSQL

#### Option A: Docker (Khuyến nghị)

```bash
# Chạy PostgreSQL container
docker run --name postgres-weather \
  -e POSTGRES_DB=weather_db \
  -e POSTGRES_USER=weather_user \
  -e POSTGRES_PASSWORD=weather_pass \
  -p 5432:5432 \
  -d postgres:15

# Kiểm tra container đang chạy
docker ps | grep postgres-weather
```

#### Option B: Cài Đặt Local

1. Tải và cài đặt PostgreSQL từ https://www.postgresql.org/download/
2. Tạo database và user:

```sql
CREATE DATABASE weather_db;
CREATE USER weather_user WITH PASSWORD 'weather_pass';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user;
```

### Bước 2: Tạo Bảng PostgreSQL

Kết nối vào PostgreSQL và chạy script sau:

```sql
-- Kết nối vào database
\c weather_db

-- Tạo bảng forecasts
CREATE TABLE IF NOT EXISTS weather_forecasts (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_datetime TIMESTAMP NOT NULL,
    temperature_celsius DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    wind_direction_deg DOUBLE PRECISION,
    model_version VARCHAR(50),
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confidence_score DOUBLE PRECISION,
    UNIQUE(city, forecast_date, forecast_datetime)
);

-- Tạo indexes để query nhanh hơn
CREATE INDEX IF NOT EXISTS idx_forecast_city_date ON weather_forecasts(city, forecast_date);
CREATE INDEX IF NOT EXISTS idx_forecast_datetime ON weather_forecasts(forecast_datetime);

-- Cấp quyền cho user
GRANT ALL PRIVILEGES ON TABLE weather_forecasts TO weather_user;
GRANT USAGE, SELECT ON SEQUENCE weather_forecasts_id_seq TO weather_user;
```

Hoặc sử dụng Python script:

```python
import psycopg2
import sys
sys.path.append('../spark_etl_weather_disaster')
import postgres_config

conn = psycopg2.connect(
    host=postgres_config.POSTGRES_HOST,
    port=postgres_config.POSTGRES_PORT,
    database=postgres_config.POSTGRES_DATABASE,
    user=postgres_config.POSTGRES_USER,
    password=postgres_config.POSTGRES_PASSWORD
)

cursor = conn.cursor()
cursor.execute(postgres_config.FORECAST_TABLE_SCHEMA)
conn.commit()
cursor.close()
conn.close()
print("✅ Bảng đã được tạo!")
```

### Bước 3: Kiểm Tra MinIO

Đảm bảo MinIO đang chạy và có dữ liệu:

```bash
# Kiểm tra MinIO đang chạy
curl http://localhost:9000/minio/health/live

# Hoặc truy cập web UI: http://localhost:9001
```

Đảm bảo có dữ liệu trong bucket `weather-data/enriched/weather/` (Parquet format).

### Bước 4: Cấu Hình

Kiểm tra và cập nhật các file config nếu cần:

1. **MinIO Config** (`../spark_etl_weather_disaster/minio_config.py`):
   ```python
   MINIO_ENDPOINT = "localhost:9000"
   MINIO_ACCESS_KEY = "minioadmin"
   MINIO_SECRET_KEY = "minioadmin"
   MINIO_BUCKET = "weather-data"
   ```

2. **PostgreSQL Config** (`../spark_etl_weather_disaster/postgres_config.py`):
   ```python
   POSTGRES_HOST = "localhost"
   POSTGRES_PORT = 5432
   POSTGRES_DATABASE = "weather_db"
   POSTGRES_USER = "weather_user"
   POSTGRES_PASSWORD = "weather_pass"
   ```

### Bước 5: Chạy Pipeline

#### Option A: Chạy trực tiếp với Python

```bash
cd spark-ml
python spark_lstm_forecast.py
```

#### Option B: Chạy với spark-submit

```bash
cd spark-ml
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0 \
  --driver-memory 4g \
  --executor-memory 4g \
  spark_lstm_forecast.py
```

## 📊 Cấu Trúc Mô Hình LSTM

Mô hình LSTM được xây dựng với kiến trúc sau:

```
Input Layer: (lookback_timesteps, n_features)
    ↓
LSTM Layer 1: 50 units, return_sequences=True
    ↓
Dropout: 0.2
    ↓
LSTM Layer 2: 50 units, return_sequences=True
    ↓
Dropout: 0.2
    ↓
LSTM Layer 3: 50 units
    ↓
Dropout: 0.2
    ↓
Dense Layer: n_features (output)
```

### Tham Số Mô Hình

- **Lookback Window**: 30 ngày (720 giờ nếu dữ liệu theo giờ)
- **Forecast Horizon**: 7 ngày (168 giờ)
- **Features**: temperature, humidity, pressure, wind_speed, wind_direction
- **Batch Size**: 32
- **Epochs**: 50
- **Optimizer**: Adam (learning_rate=0.001)
- **Loss Function**: MSE (Mean Squared Error)

## 🔧 Tùy Chỉnh

### Thay Đổi Số Ngày Dự Đoán

Trong `spark_lstm_forecast.py`:

```python
FORECAST_DAYS = 14  # Thay đổi từ 7 sang 14 ngày
```

### Thay Đổi Lookback Window

```python
LOOKBACK_DAYS = 60  # Tăng từ 30 lên 60 ngày
```

### Thay Đổi Features

```python
FEATURE_COLUMNS = ['temperature', 'humidity', 'pressure']  # Bỏ wind_speed và wind_direction
```

### Thay Đổi Hyperparameters

```python
BATCH_SIZE = 64  # Tăng batch size
EPOCHS = 100     # Tăng số epochs
```

## 📈 Kiểm Tra Kết Quả

### Query PostgreSQL

```sql
-- Xem tất cả dự đoán
SELECT * FROM weather_forecasts ORDER BY forecast_datetime DESC LIMIT 100;

-- Xem dự đoán cho một thành phố cụ thể
SELECT city, forecast_datetime, temperature_celsius, humidity_pct 
FROM weather_forecasts 
WHERE city = 'New York' 
ORDER BY forecast_datetime;

-- Xem dự đoán cho ngày cụ thể
SELECT city, forecast_datetime, temperature_celsius 
FROM weather_forecasts 
WHERE forecast_date = '2024-01-15'
ORDER BY city, forecast_datetime;

-- Thống kê theo thành phố
SELECT 
    city, 
    COUNT(*) as forecast_count,
    AVG(temperature_celsius) as avg_temp,
    MIN(temperature_celsius) as min_temp,
    MAX(temperature_celsius) as max_temp
FROM weather_forecasts
GROUP BY city;
```

### Visualize với Python

```python
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import sys
sys.path.append('../spark_etl_weather_disaster')
import postgres_config

# Kết nối PostgreSQL
conn = psycopg2.connect(
    host=postgres_config.POSTGRES_HOST,
    port=postgres_config.POSTGRES_PORT,
    database=postgres_config.POSTGRES_DATABASE,
    user=postgres_config.POSTGRES_USER,
    password=postgres_config.POSTGRES_PASSWORD
)

# Đọc dữ liệu
query = """
SELECT forecast_datetime, temperature_celsius, humidity_pct 
FROM weather_forecasts 
WHERE city = 'New York'
ORDER BY forecast_datetime
"""

df = pd.read_sql(query, conn)
conn.close()

# Vẽ biểu đồ
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

ax1.plot(df['forecast_datetime'], df['temperature_celsius'])
ax1.set_title('Temperature Forecast')
ax1.set_xlabel('Date')
ax1.set_ylabel('Temperature (°C)')
ax1.grid(True)

ax2.plot(df['forecast_datetime'], df['humidity_pct'])
ax2.set_title('Humidity Forecast')
ax2.set_xlabel('Date')
ax2.set_ylabel('Humidity (%)')
ax2.grid(True)

plt.tight_layout()
plt.show()
```

## 🐛 Xử Lý Lỗi Thường Gặp

### 1. Lỗi Kết Nối MinIO

```
❌ Lỗi đọc dữ liệu từ MinIO: ...
```

**Giải pháp:**
- Kiểm tra MinIO server đang chạy: `docker ps | grep minio`
- Kiểm tra endpoint trong `minio_config.py`
- Kiểm tra credentials (access key, secret key)
- Kiểm tra bucket và folder tồn tại

### 2. Lỗi Kết Nối PostgreSQL

```
❌ Lỗi ghi vào PostgreSQL: ...
```

**Giải pháp:**
- Kiểm tra PostgreSQL đang chạy: `docker ps | grep postgres`
- Kiểm tra credentials trong `postgres_config.py`
- Kiểm tra database và table đã được tạo
- Kiểm tra PostgreSQL driver trong Spark classpath

### 3. Lỗi Không Đủ Dữ Liệu

```
❌ Không đủ dữ liệu để tạo sequences
```

**Giải pháp:**
- Giảm `LOOKBACK_DAYS` trong config
- Đảm bảo có đủ dữ liệu trong MinIO (ít nhất vài tháng)
- Kiểm tra dữ liệu có bị thiếu không

### 4. Lỗi Memory

```
OutOfMemoryError: Java heap space
```

**Giải pháp:**
- Tăng driver memory: `--driver-memory 8g`
- Giảm số thành phố xử lý cùng lúc
- Xử lý từng thành phố một thay vì batch

### 5. Lỗi TensorFlow

```
⚠️ TensorFlow không được cài đặt
```

**Giải pháp:**
```bash
pip install tensorflow>=2.10.0
```

## 🔄 Tích Hợp với Airflow (Tùy chọn)

Để tự động hóa pipeline, có thể tạo Airflow DAG:

```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_lstm_forecast',
    default_args=default_args,
    description='Dự đoán thời tiết 7 ngày với LSTM',
    schedule_interval=timedelta(days=1),  # Chạy mỗi ngày
    catchup=False,
)

forecast_task = BashOperator(
    task_id='run_lstm_forecast',
    bash_command='cd /path/to/spark-ml && python spark_lstm_forecast.py',
    dag=dag,
)

forecast_task
```

## 📚 Tài Liệu Tham Khảo

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [TensorFlow/Keras LSTM Guide](https://www.tensorflow.org/guide/keras/rnn)
- [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/)
- [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/API.html)

## 📝 Ghi Chú

- Mô hình LSTM được huấn luyện lại mỗi lần chạy (có thể tối ưu bằng cách lưu model)
- Dự đoán được thực hiện theo từng giờ (hourly)
- Có thể mở rộng để dự đoán nhiều thành phố song song
- Có thể tích hợp với Grafana để visualize real-time

## ✅ Checklist Trước Khi Chạy

- [ ] MinIO server đang chạy và có dữ liệu
- [ ] PostgreSQL server đang chạy
- [ ] Database và table đã được tạo
- [ ] Tất cả dependencies đã được cài đặt
- [ ] Config files đã được cập nhật đúng
- [ ] Có đủ RAM (ít nhất 4GB cho Spark driver)

---

**Tác giả**: Weather Analysis Team  
**Ngày cập nhật**: 2024-01-15

