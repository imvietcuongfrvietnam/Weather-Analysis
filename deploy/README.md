# 📦 Deploy - Triển khai hệ thống phân tích thời tiết

Thư mục này chứa các file cấu hình triển khai cho toàn bộ hệ thống: Airflow, Kafka, MinIO, PostgreSQL, Redis, Spark, Streamlit.

## ✨ Chức năng chính
- Cấu hình và khởi tạo các service cần thiết cho pipeline phân tích và dự báo thời tiết.
- Hỗ trợ triển khai bằng Docker Compose hoặc Kubernetes.
- Quản lý tài nguyên, môi trường và thông số kết nối.

## 📁 Cấu trúc thư mục
```
deploy/
├── airflow.yaml         # Cấu hình Airflow
├── kafka.yaml           # Cấu hình Kafka
├── minio.yaml           # Cấu hình MinIO (lưu trữ dữ liệu)
├── postgre.yaml         # Cấu hình PostgreSQL
├── redis.yaml           # Cấu hình Redis (real-time)
├── spark.yaml           # Cấu hình Spark
├── streamlit.yaml       # Cấu hình dashboard Streamlit
└── README.md            # Tài liệu này
```

## 🚀 Hướng dẫn sử dụng
1. Sửa các file YAML cho phù hợp với môi trường (port, volume, biến môi trường).
2. Khởi động các service bằng Kubenetes:
minikube start

kubectl apply -f .
   ...
   ```
3. Kiểm tra trạng thái các service bằng lệnh:
   ```powershell
   docker ps
   ```

## ⚙️ Lưu ý cấu hình
- Đảm bảo các port không bị trùng lặp với các ứng dụng khác.
- Sử dụng biến môi trường để bảo mật thông tin truy cập.
- Có thể triển khai trên cloud hoặc server vật lý tùy nhu cầu.

## 📝 Ghi chú
- Có thể mở rộng thêm các file cấu hình cho các service mới.
- Tham khảo tài liệu từng service để tối ưu cấu hình.

---
Triển khai đồng bộ giúp hệ thống hoạt động ổn định và hiệu quả!