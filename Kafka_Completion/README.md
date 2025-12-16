## Pipeline dữ liệu NYC với Kafka – Phần Producer

Phần này của dự án triển khai **các Producer Kafka** để đẩy dữ liệu từ các file CSV vào các topic Kafka, theo mô tả trong slide tổng quan pipeline (`tong_quan_pipeline.pdf`).

### 1. Cấu trúc thư mục đề xuất

- `config/`
  - `kafka_config.py` – cấu hình Kafka dùng chung (bootstrap servers, tên topic, tốc độ gửi, v.v.)
- `producers/`
  - `base_producer.py` – lớp tiện ích chung để đọc CSV và gửi lên Kafka
  - `producer_311.py` – producer cho dữ liệu 311 (`nyc_311_2016_full.csv`)
  - `producer_collisions.py` – producer cho dữ liệu tai nạn (`nyc_collisions_2016.csv`)
  - `producer_emergency_alerts.py` – producer cho dữ liệu cảnh báo thời tiết / thiên tai (`nyc_emergency_alerts_2012_2017_full.csv`)
- `scripts/`
  - `create_topics.py` – script tạo các topic Kafka cần thiết
- `requirements.txt` – khai báo thư viện Python

Bạn có thể chỉnh sửa lại tên file / topic cho phù hợp với yêu cầu cụ thể của môn học.

### 2. Cài đặt môi trường

```bash
pip install -r requirements.txt
```

### 3. Chạy Kafka Producer

1. Đảm bảo Kafka đang chạy (có thể bằng Docker hoặc cluster sẵn có).
2. Cập nhật thông tin `BOOTSTRAP_SERVERS` trong `config/kafka_config.py`.
3. Tạo các topic:

```bash
python scripts/create_topics.py
```

4. Chạy từng producer (ví dụ 311 complaints):

```bash
python producers/producer_311.py
```

Các producer khác chạy tương tự, chỉ khác file CSV và tên topic.


