import time
import json
import random
import sys
import csv
import os
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# --- CẤU HÌNH ---
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'nyc-weather-raw' 
LOCATIONS = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten_Island"]
DATA_FILE = 'data/data_weather.csv' 

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
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.gauss(base_temp, 5), 2),
        "humidity": round(random.uniform(30, 90), 1),
        "pressure": round(random.uniform(1000, 1025), 1),
        "weather_desc": random.choice(WEATHER_OPTS),
        "wind_speed": round(random.uniform(0, 10) + (5 if is_coastal else 0), 1),
        "wind_direction": round(random.uniform(0, 360), 0),
        "location": location_name
    }

def safe_float(value):
    """Hàm phụ trợ: Chuyển string sang float, nếu lỗi hoặc rỗng thì trả về None"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# --- 3. CHƯƠNG TRÌNH CHÍNH ---
def run_producer(mode='mock'):
    # Cấu hình Producer tối ưu
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip'
    )
    
    print(f"🚀 BẮT ĐẦU BƠM DỮ LIỆU ({mode.upper()}) VÀO TOPIC: {TOPIC_NAME}")
    print("👉 Nhấn Ctrl+C để dừng lại.")

    try:
        if mode == 'mock':
            while True:
                for loc in LOCATIONS:
                    mock_data = generate_mock_record(loc)
                    producer.send(TOPIC_NAME, key=loc, value=mock_data)
                    if loc == "Manhattan":
                        print(f"📤 [Mock {loc}] Temp: {mock_data['temperature']}°C | {mock_data['weather_desc']}")
                time.sleep(1)
        
        elif mode == 'csv':
            if not os.path.exists(DATA_FILE):
                print(f"❌ Lỗi: Không tìm thấy file tại {DATA_FILE}")
                return
            with open(DATA_FILE, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                count = 0
                for row in reader:
                    message = {
                        "datetime": row['datetime'],
                        "location": row.get('City', 'Unknown'),
                        "temperature": safe_float(row['temperature']),
                        "humidity": safe_float(row['humidity']),
                        "pressure": safe_float(row['pressure']),
                        "weather_desc": row['weather_desc'],
                        "wind_direction": safe_float(row['wind_direction']),
                        "wind_speed": safe_float(row['wind_speed'])
                    }
                    producer.send(TOPIC_NAME, key=message['location'], value=message)
                    count += 1
                    print(f"[{count}] 📤 CSV: {message['location']} ({message['datetime']}): {message['temperature']}°C")
                    time.sleep(0.2)

    except KeyboardInterrupt:
        print("\n🛑 Đang dừng Producer...")
    except Exception as e:
        print(f"❌ Có lỗi xảy ra: {e}")
    finally:
        producer.flush()
        producer.close()
        print("✅ Đã đóng kết nối Kafka.")

if __name__ == "__main__":
    create_topic()
    # Mặc định chạy mock mode, có thể đổi sang 'csv'
    run_producer(mode='mock')