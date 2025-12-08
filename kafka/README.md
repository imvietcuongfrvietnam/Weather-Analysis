# 📦 Kafka - Streaming dữ liệu thời tiết

Thư mục này chứa các script và tài liệu tích hợp Kafka cho pipeline thời tiết.

## ✨ Chức năng chính
- Streaming dữ liệu thời tiết từ các nguồn vào Kafka topic.
- Hỗ trợ tích hợp với Spark, Airflow, và các service khác.
- Quản lý topic, producer, consumer cho dữ liệu thời gian thực.

## 📁 Cấu trúc thư mục
```
kafka/
├── weather_kafka.py     # Script gửi dữ liệu thời tiết vào Kafka
├── README.md            # Tài liệu này
```

## 🚀 Hướng dẫn sử dụng
1. Đảm bảo Kafka đã được khởi động:
   ```powershell
   kubectl port-forward svc/weather-kafka 9094:9094 -n default
   ```
2. Chỉnh sửa script `weather_kafka.py` để phù hợp với cấu hình topic và nguồn dữ liệu.
3. Chạy script để gửi dữ liệu:
   ```powershell
   python weather_kafka.py
   ```
4. Kiểm tra dữ liệu bằng consumer hoặc tích hợp với Spark Streaming.

## ⚙️ Lưu ý cấu hình
- Sửa thông số kết nối Kafka (host, port, topic) trong script cho đúng môi trường.
- Có thể mở rộng thêm các topic cho các loại dữ liệu khác nhau.

## 📝 Ghi chú
- Kafka là nền tảng truyền tải dữ liệu thời gian thực cho toàn bộ hệ thống.
- Theo dõi log để kiểm tra tiến trình và xử lý lỗi.

---
Kafka giúp hệ thống xử lý dữ liệu thời tiết nhanh chóng và linh hoạt!
#