# Hướng Dẫn Thực Hiện: Dự Đoán Thời Tiết 7 Ngày Bằng LSTM

## 📋 Tổng Quan Nhiệm Vụ

Bạn cần thực hiện pipeline:
1. **Đọc dữ liệu từ MinIO** (dữ liệu đã được xử lý từ ETL pipeline)
2. **Chạy SparkML với LSTM** để dự đoán thời tiết 7 ngày tới
3. **Ghi kết quả vào PostgreSQL**

## 📁 Các File Đã Được Tạo

### 1. Cấu Hình PostgreSQL
- **File**: `spark_etl_weather_disaster/postgres_config.py`
- **Chức năng**: Cấu hình kết nối PostgreSQL, định nghĩa schema bảng

### 2. Script Chính - LSTM Forecast
- **File**: `spark-ml/spark_lstm_forecast.py`
- **Chức năng**: 
  - Đọc dữ liệu từ MinIO
  - Chuẩn bị dữ liệu time series
  - Huấn luyện mô hình LSTM
  - Dự đoán 7 ngày tới
  - Ghi kết quả vào PostgreSQL

### 3. PostgreSQL Writer Utility
- **File**: `spark_etl_weather_disaster/writers/postgres_data_writer.py`
- **Chức năng**: Utility class để ghi dữ liệu vào PostgreSQL

### 4. Script Setup PostgreSQL
- **File**: `spark-ml/setup_postgres_table.py`
- **Chức năng**: Tạo bảng PostgreSQL tự động

### 5. Hướng Dẫn Chi Tiết
- **File**: `spark-ml/LSTM_FORECAST_GUIDE.md`
- **Chức năng**: Hướng dẫn đầy đủ bằng tiếng Anh

## 🚀 Các Bước Thực Hiện

### Bước 1: Cài Đặt Dependencies

```bash
cd Weather-Analysis/spark_etl_weather_disaster
pip install -r requirements.txt
```

Các package chính:
- `pyspark>=3.3.0`
- `tensorflow>=2.10.0` (cho LSTM)
- `scikit-learn>=1.0.0`
- `psycopg2-binary>=2.9.0` (cho PostgreSQL)
- `pandas`, `numpy`, `minio`, `pyarrow`

### Bước 2: Setup PostgreSQL

#### Option A: Docker (Khuyến nghị)

```bash
docker run --name postgres-weather \
  -e POSTGRES_DB=weather_db \
  -e POSTGRES_USER=weather_user \
  -e POSTGRES_PASSWORD=weather_pass \
  -p 5432:5432 \
  -d postgres:15
```

#### Option B: Cài Đặt Local

1. Tải PostgreSQL từ https://www.postgresql.org/download/
2. Tạo database và user:

```sql
CREATE DATABASE weather_db;
CREATE USER weather_user WITH PASSWORD 'weather_pass';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user;
```

### Bước 3: Tạo Bảng PostgreSQL

```bash
cd Weather-Analysis/spark-ml
python setup_postgres_table.py
```

Hoặc chạy SQL thủ công:

```sql
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
```

### Bước 4: Kiểm Tra MinIO

Đảm bảo:
- MinIO server đang chạy (localhost:9000)
- Có dữ liệu trong bucket `weather-data/enriched/weather/` (format Parquet)
- Nếu chưa có, chạy ETL pipeline trước: `python spark_etl_weather_disaster/main_etl.py`

### Bước 5: Cấu Hình (Nếu Cần)

Kiểm tra và cập nhật các file config:

**MinIO Config** (`spark_etl_weather_disaster/minio_config.py`):
```python
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "weather-data"
```

**PostgreSQL Config** (`spark_etl_weather_disaster/postgres_config.py`):
```python
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "weather_db"
POSTGRES_USER = "weather_user"
POSTGRES_PASSWORD = "weather_pass"
```

### Bước 6: Chạy LSTM Forecast

#### Option A: Chạy với Python (Đơn giản)

```bash
cd Weather-Analysis/spark-ml
python spark_lstm_forecast.py
```

#### Option B: Chạy với spark-submit (Production)

```bash
cd Weather-Analysis/spark-ml
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0 \
  --driver-memory 4g \
  --executor-memory 4g \
  spark_lstm_forecast.py
```

## 📊 Kết Quả

Sau khi chạy thành công, dữ liệu dự đoán sẽ được lưu vào bảng `weather_forecasts` trong PostgreSQL.

### Kiểm Tra Kết Quả

```sql
-- Xem tất cả dự đoán
SELECT * FROM weather_forecasts 
ORDER BY prediction_timestamp DESC 
LIMIT 100;

-- Xem dự đoán cho một thành phố cụ thể
SELECT 
    forecast_datetime, 
    temperature_celsius, 
    humidity_pct,
    wind_speed_kmh
FROM weather_forecasts 
WHERE city = 'New York'
ORDER BY forecast_datetime;

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

## 🔧 Tùy Chỉnh

### Thay Đổi Số Ngày Dự Đoán

Trong `spark-ml/spark_lstm_forecast.py`:

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
BATCH_SIZE = 64   # Tăng batch size
EPOCHS = 100      # Tăng số epochs
```

## 🐛 Xử Lý Lỗi

### Lỗi: Không kết nối được MinIO
- Kiểm tra MinIO đang chạy: `docker ps | grep minio`
- Kiểm tra endpoint và credentials trong `minio_config.py`
- Kiểm tra bucket và folder tồn tại

### Lỗi: Không kết nối được PostgreSQL
- Kiểm tra PostgreSQL đang chạy: `docker ps | grep postgres`
- Kiểm tra credentials trong `postgres_config.py`
- Kiểm tra database và table đã được tạo

### Lỗi: Không đủ dữ liệu
- Giảm `LOOKBACK_DAYS` trong config
- Đảm bảo có đủ dữ liệu trong MinIO (ít nhất vài tháng)
- Kiểm tra dữ liệu có bị thiếu không

### Lỗi: OutOfMemoryError
- Tăng driver memory: `--driver-memory 8g`
- Giảm số thành phố xử lý cùng lúc
- Xử lý từng thành phố một thay vì batch

### Lỗi: TensorFlow không được cài đặt
```bash
pip install tensorflow>=2.10.0
```

## 📈 Kiến Trúc Mô Hình LSTM

Mô hình sử dụng kiến trúc:
- **3 LSTM layers** (50 units mỗi layer)
- **Dropout** (0.2) để tránh overfitting
- **Dense layer** để output tất cả features cùng lúc
- **Optimizer**: Adam (learning_rate=0.001)
- **Loss**: MSE (Mean Squared Error)

## 🔄 Tích Hợp với Airflow (Tùy chọn)

Để tự động hóa, có thể tạo Airflow DAG để chạy hàng ngày:

```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'weather_lstm_forecast',
    schedule_interval=timedelta(days=1),  # Chạy mỗi ngày
    start_date=datetime(2024, 1, 1),
)

forecast_task = BashOperator(
    task_id='run_lstm_forecast',
    bash_command='cd /path/to/spark-ml && python spark_lstm_forecast.py',
    dag=dag,
)
```

## ✅ Checklist

Trước khi chạy, đảm bảo:

- [ ] MinIO server đang chạy và có dữ liệu
- [ ] PostgreSQL server đang chạy
- [ ] Database và table đã được tạo
- [ ] Tất cả dependencies đã được cài đặt
- [ ] Config files đã được cập nhật đúng
- [ ] Có đủ RAM (ít nhất 4GB cho Spark driver)
- [ ] TensorFlow đã được cài đặt

## 📚 Tài Liệu Tham Khảo

- Xem `spark-ml/LSTM_FORECAST_GUIDE.md` để biết hướng dẫn chi tiết hơn
- Xem `spark-ml/README.md` để biết quick start guide
- Pipeline diagram: Xem ảnh đính kèm trong yêu cầu

## 💡 Lưu Ý

1. **Mô hình được huấn luyện lại mỗi lần chạy** - Có thể tối ưu bằng cách lưu model và load lại
2. **Dự đoán theo từng giờ** - Có thể thay đổi thành daily nếu cần
3. **Xử lý từng thành phố** - Có thể mở rộng để xử lý song song nhiều thành phố
4. **Kết quả được append vào PostgreSQL** - Có thể có duplicates, sẽ được xử lý bởi UNIQUE constraint

## 🎯 Kết Luận

Pipeline này hoàn chỉnh và sẵn sàng sử dụng. Chỉ cần:
1. Setup PostgreSQL
2. Đảm bảo có dữ liệu trong MinIO
3. Chạy script `spark_lstm_forecast.py`

Kết quả sẽ được lưu vào PostgreSQL và có thể được visualize bằng Grafana hoặc các công cụ khác.

---

**Chúc bạn thành công!** 🚀

