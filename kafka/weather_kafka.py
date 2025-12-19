import time
import json
import csv
import os
from kafka import KafkaProducer

# --- CẤU HÌNH ---
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'weather'
DATA_FILE = 'data/data_weather.csv' 
DELAY_SECONDS = 0.2

# --- KHỞI TẠO PRODUCER ---
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def run_producer():
    print(f"🚀 BẮT ĐẦU STREAMING TỪ FILE: {DATA_FILE}")
    
    if not os.path.exists(DATA_FILE):
        print(f"❌ Lỗi: Không tìm thấy file {DATA_FILE}. Hãy chạy script preprocess trước!")
        return

    try:
        with open(DATA_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            count = 0
            for row in reader:
                # 1. Chuẩn hóa dữ liệu
                message = {
                    "datetime": row['datetime'],
                    "City": row['City'],
                    "temperature": safe_float(row['temperature']),
                    "humidity": safe_float(row['humidity']),
                    "pressure": safe_float(row['pressure']),
                    "weather_desc": row['weather_desc'],
                    "wind_direction": safe_float(row['wind_direction']),
                    "wind_speed": safe_float(row['wind_speed'])
                }

                # 2. Gửi vào Kafka (Key là City để chia partition)
                producer.send(TOPIC_NAME, key=message['City'], value=message)
                
                count += 1
                print(f"[{count}] 📤 {message['datetime']} | {message['City']} | {message['temperature']}°C")

                # 3. Nghỉ 30 giây
                time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n🛑 Đã dừng Producer.")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()