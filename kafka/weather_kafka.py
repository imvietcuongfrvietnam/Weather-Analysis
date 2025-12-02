import time
import json
import random
import sys
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# --- CẤU HÌNH ---
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'nyc-weather-raw' 
LOCATIONS = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten_Island"]

# Danh sách mô tả thời tiết (Lấy từ dữ liệu mẫu của NYC)
WEATHER_OPTS = [
    "sky is clear", "few clouds", "scattered clouds", "broken clouds",
    "overcast clouds", "light rain", "moderate rain", "heavy intensity rain",
    "mist", "haze", "fog", "thunderstorm"
]

# --- 1. SETUP TOPIC (Tự động tạo nếu chưa có) ---
def create_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        # Tạo topic với 3 partitions để demo song song
        topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"✅ Đã tạo topic '{TOPIC_NAME}'")
    except TopicAlreadyExistsError:
        print(f"⚠️ Topic '{TOPIC_NAME}' đã có sẵn. Sẵn sàng bắn data.")
    except Exception as e:
        print(f"❌ Lỗi kiểm tra topic: {e}")
    finally:
        admin_client.close()

# --- 2. HÀM SINH DỮ LIỆU GIỐNG HỆT CSV ---
def generate_mock_record(location_name):
    # Giả lập nhiệt độ cơ bản (khoảng 15 độ C)
    base_temp = 15.0 
    
    # Tạo biến động nhẹ tùy theo quận (Ví dụ: Gần biển thì gió to hơn)
    is_coastal = location_name in ["Queens", "Brooklyn", "Staten_Island"]
    
    return {
        # --- CÁC TRƯỜNG DỮ LIỆU GỐC (Original Schema) ---
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.gauss(base_temp, 5), 2),
        "humidity": round(random.uniform(30, 90), 1),
        "pressure": round(random.uniform(1000, 1025), 1),
        "weather_desc": random.choice(WEATHER_OPTS),
        "wind_speed": round(random.uniform(0, 10) + (5 if is_coastal else 0), 1),
        "wind_direction": round(random.uniform(0, 360), 0),
        
        # --- TRƯỜNG BỔ SUNG LÀM KEY (Enrichment) ---
        "location": location_name
    }

# --- 3. CHƯƠNG TRÌNH CHÍNH ---
def run_producer():
    # Cấu hình Producer tối ưu (Batching + Compression)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'), # Key phải là bytes
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip'
    )
    
    print(f"🚀 BẮT ĐẦU BƠM DỮ LIỆU VÀO TOPIC: {TOPIC_NAME}")
    print("👉 Nhấn Ctrl+C để dừng lại.")

    try:
        while True:
            for loc in LOCATIONS:
                # 1. Tạo data ảo
                mock_data = generate_mock_record(loc)
                
                try:
                    # 2. Gửi vào Kafka
                    # Quan trọng: Key=loc để Kafka chia partition theo quận
                    future = producer.send(TOPIC_NAME, key=loc, value=mock_data)
                    
                    # (Tùy chọn) In log ra màn hình để nhìn cho sướng mắt
                    # Chỉ in đại diện Manhattan để đỡ trôi màn hình quá nhanh
                    if loc == "Manhattan":
                        print(f"📤 [Gửi {loc}] Temp: {mock_data['temperature']}°C | {mock_data['weather_desc']}")
                        
                except KafkaError as e:
                    print(f"❌ Lỗi gửi: {e}")

            # 3. Giả lập thời gian thực (1 giây cập nhật 1 lần cho cả 5 quận)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Đang dừng Producer...")
        producer.flush()
        producer.close()
        print("✅ Đã tắt thành công.")

if __name__ == "__main__":
    create_topic()
    run_producer()