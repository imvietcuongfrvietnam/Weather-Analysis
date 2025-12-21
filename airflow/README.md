# 📦 Airflow - Quản lý Pipeline Dự báo Thời tiết

Thư mục này chứa các DAG và job của Airflow dùng để tự động hóa quy trình xử lý, dự báo và phân tích dữ liệu thời tiết.

## ✨ Chức năng chính
- Tự động hóa ETL, huấn luyện mô hình, dự báo thời tiết.
- Quản lý các pipeline batch và streaming với Spark.
- Lên lịch chạy định kỳ, giám sát và xử lý lỗi.

## 📁 Cấu trúc thư mục
```
airflow/
├── spark_ml_batch_job.py        # Job batch ML với Spark
├── spark_streaming_job.py       # Job streaming dữ liệu thời tiết
├── weather_master_dag.py        # DAG tổng hợp quản lý toàn bộ pipeline
└── README.md                    # Tài liệu này
```

## 🚀 Hướng dẫn sử dụng
1. Cài đặt Airflow:
   ```powershell
   pip install apache-airflow
   ```
2. Khởi động Airflow:
   ```powershell
   airflow db init
   airflow webserver -p 8080
   airflow scheduler
   ```
3. Copy các file DAG vào thư mục `dags/` của Airflow.
4. Truy cập giao diện web tại http://localhost:8080 để quản lý pipeline.

## ⚙️ Lưu ý cấu hình
- Đảm bảo các service như Spark, Kafka, MinIO, PostgreSQL đã chạy trước khi kích hoạt DAG.
- Sửa các đường dẫn, thông số kết nối trong các file job cho phù hợp môi trường thực tế.

## 📝 Ghi chú
- Có thể mở rộng thêm các DAG cho các tác vụ mới (ví dụ: cảnh báo thiên tai, phân tích lịch sử).
- Theo dõi log Airflow để kiểm tra tiến trình và xử lý lỗi.

---
Airflow giúp tự động hóa toàn bộ quy trình phân tích và dự báo thời tiết!
