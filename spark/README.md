# ⚡ Spark Processing Engine - Weather Analysis Project

Module này là trái tim của hệ thống Big Data, chịu trách nhiệm xử lý dữ liệu theo kiến trúc **Lambda Architecture**. Nó bao gồm các tác vụ xử lý luồng (Streaming ETL) và xử lý lô (Batch Machine Learning).

## 🏗️ Kiến trúc & Luồng dữ liệu

Hệ thống Spark được chia thành 2 luồng xử lý chính:

1.  **Speed Layer (Streaming Job):**
    * Đọc dữ liệu Real-time từ **Kafka**.
    * Làm sạch (Cleaning) và chuẩn hóa (Normalization).
    * Ghi dữ liệu nóng vào **Redis** (cho Dashboard Realtime).
    * Ghi dữ liệu lạnh vào **MinIO** (dạng Parquet) để lưu trữ lịch sử.

2.  **Batch Layer (ML Job):**
    * Đọc dữ liệu lịch sử từ **MinIO**.
    * Feature Engineering (tạo lag features, rolling windows).
    * Huấn luyện mô hình (Regression/Classification) sử dụng **Spark MLlib**.
    * Lưu Model vào MinIO và ghi kết quả dự báo vào **PostgreSQL**.

---

## 📂 Cấu trúc thư mục

spark/
├── config/                 # Cấu hình hệ thống (MinIO, Kafka, Redis, Postgres)
│   └── config.py
├── job/                    # Các Spark Job chính (Entry points)
│   ├── main_etl.py         # Job Streaming ETL (Kafka -> MinIO/Redis)
│   └── spark_ml_job.py     # Job Batch ML (Training & Forecasting)
├── readers/                # Modules đọc dữ liệu
│   └── real_data_reader.py # Đọc từ Kafka/MinIO
├── writers/                # Modules ghi dữ liệu
│   ├── minio_writer.py     # Ghi Parquet xuống Data Lake
│   ├── redis_data_writer.py# Ghi xuống Redis
│   └── postgres_writer.py  # Ghi kết quả dự báo xuống DB
├── transformations/        # Logic biến đổi dữ liệu
│   ├── cleaning.py         # Làm sạch, xử lý null
│   └── normalization.py    # Chuẩn hóa dữ liệu
├── schemas/                # Định nghĩa Schema (StructType)
│   └── data_schemas.py
├── feature_engineering.py  # Tạo đặc trưng cho ML
├── models.py               # Định nghĩa các thuật toán ML
├── data_loader.py          # Helper load dữ liệu cho ML
├── connection_utils.py     # Tiện ích kết nối
└── requirements.txt        # Các thư viện Python cần thiết

## 🚀 Hướng dẫn chạy (Deployment)

Mã nguồn này được thiết kế để chạy trên môi trường **Kubernetes** thông qua **Apache Airflow**, nhưng cũng có thể chạy Local để kiểm thử.

### 1. Yêu cầu môi trường (Prerequisites)
* Python 3.9+
* Apache Spark 3.x
* Java 11 (cho Spark)
* Các dịch vụ phụ trợ đang chạy: Kafka, MinIO, Redis, PostgreSQL.

### 2. Cài đặt thư viện
pip install -r requirements.txt

### 3. Chạy Streaming Job (ETL)
Job này sẽ chạy vô hạn, lắng nghe Kafka và đẩy dữ liệu đi.

# Chạy local
python job/main_etl.py

# Chạy với spark-submit (Production)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  job/main_etl.py

### 4. Chạy Batch Job (Machine Learning)
Job này sẽ train model dựa trên dữ liệu hiện có trong MinIO.

# Chạy local
python job/spark_ml_job.py

# Chạy với spark-submit
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0 \
  job/spark_ml_job.py

---

## ⚙️ Cấu hình (Configuration)

Toàn bộ cấu hình được quản lý tập trung tại `config/config.py`. Bạn có thể thay đổi các thông số sau bằng biến môi trường hoặc sửa trực tiếp file:

| Tham số | Mô tả | Mặc định |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | Địa chỉ Kafka Broker | `my-cluster-kafka-bootstrap:9092` |
| `MINIO_ENDPOINT` | Endpoint của MinIO | `weather-minio:9000` |
| `REDIS_HOST` | Địa chỉ Redis | `weather-redis` |
| `POSTGRES_HOST` | Địa chỉ PostgreSQL | `weather-postgresql` |
| `SPARK_MASTER` | Spark Master URL | `local[*]` (hoặc `spark://...`) |

---

## 🛠️ Các module chính

### `transformations/`
Chứa các hàm Pure Functions để biến đổi DataFrame:
- **`clean_data(df)`**: Xử lý giá trị NULL, ép kiểu dữ liệu.
- **`normalize_data(df)`**: Chuẩn hóa tên thành phố, đơn vị đo lường.

### `feature_engineering.py`
Tạo các đặc trưng nâng cao cho Machine Learning:
- **Lag Features**: Nhiệt độ của 1h, 3h trước.
- **Rolling Window**: Trung bình trượt của 3h gần nhất.
- **Time Components**: Trích xuất giờ, ngày, tháng, mùa từ timestamp.

### `models.py`
Quản lý vòng đời của Model:
- **Train**: Hỗ trợ GBTRegressor, RandomForestRegressor.
- **Save/Load**: Lưu model đã train xuống MinIO để tái sử dụng.
- **Evaluate**: Tính toán RMSE, MAE, R2.

---

## 📝 Troubleshooting (Gỡ lỗi thường gặp)

1.  **Lỗi `S3AFileSystem: The specified bucket does not exist`**:
    * Đảm bảo bucket `weather-data` đã được tạo trên MinIO.

2.  **Lỗi `ConnectionRefused` tới Kafka/Redis**:
    * Kiểm tra lại `config.py`.
    * Nếu chạy trên K8s: Dùng Service Name (ví dụ `weather-redis`).
    * Nếu chạy Local: Dùng `localhost` và Port-forwarding.

3.  **Lỗi `AnalysisException: Path does not exist` khi chạy ML Job**:
    * Do Streaming Job chưa chạy hoặc chưa ghi đủ dữ liệu xuống MinIO. Hãy chạy Streaming Job trước ít nhất 5 phút để có dữ liệu.

