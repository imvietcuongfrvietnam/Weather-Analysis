import os
from dotenv import load_dotenv

# Tải biến môi trường từ file .env (nếu có)
load_dotenv()

# Địa chỉ Kafka broker, ví dụ: "localhost:9092"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Tên các topic cho từng loại dữ liệu
TOPIC_311 = os.getenv("KAFKA_TOPIC_311", "nyc_311_2016")
TOPIC_COLLISIONS = os.getenv("KAFKA_TOPIC_COLLISIONS", "nyc_collisions_2016")
TOPIC_EMERGENCY_ALERTS = os.getenv("KAFKA_TOPIC_EMERGENCY_ALERTS", "nyc_emergency_alerts_2012_2017")

# Cấu hình gửi dữ liệu
PRODUCER_FLUSH_INTERVAL = int(os.getenv("PRODUCER_FLUSH_INTERVAL", "1000"))  # số record gửi rồi flush
PRODUCER_SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.0"))  # delay giữa các bản ghi (giả lập stream)


