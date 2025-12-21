import time
import json
import csv
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
from dotenv import load_dotenv

# --- 1. LOAD CẤU HÌNH TỪ .ENV TẠI CHỖ ---
# Tải các biến môi trường từ file .env nằm cùng thư mục với script này
load_dotenv() 

# Đọc cấu hình (Sửa lỗi đặt tên biến không đồng nhất)
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')
TOPIC_NAME = os.getenv('KAFKA_TOPIC_WEATHER', 'weather')
DATA_FILE = os.getenv('DATA_FILE_DIR', '../data/data_weather.csv')
DELAY_SECONDS = 0.5 

# --- 2. KHỞI TẠO PRODUCER ---
try:
    producer = KafkaProducer(
        # Đã sửa: Truyền đúng biến KAFKA_SERVER vừa lấy từ os.getenv
        bootstrap_servers=[KAFKA_SERVER], 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        request_timeout_ms=10000 
    )
    print(f"✅ Đã kết nối tới Kafka tại: {KAFKA_SERVER}")
    print(f"📡 Topic: {TOPIC_NAME}")
except Exception as e:
    print(f"❌ Lỗi kết nối Kafka: {e}")
    exit(1)

def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def run_producer():
    print(f"🚀 BẮT ĐẦU STREAMING TỪ FILE: {DATA_FILE}")
    
    # Kiểm tra file dữ liệu
    if not os.path.exists(DATA_FILE):
        print(f"❌ Lỗi: Không tìm thấy file {DATA_FILE}.")
        print(f"📍 Bạn đang chạy script từ: {os.getcwd()}")
        return

    try:
        with open(DATA_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            count = 0
            for row in reader:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Chuẩn hóa dữ liệu gửi đi
                message = {
                    "datetime": current_time, 
                    "City": row['City'],
                    "temperature": safe_float(row['temperature']),
                    "humidity": safe_float(row['humidity']),
                    "pressure": safe_float(row['pressure']),
                    "weather_desc": row['weather_desc'],
                    "wind_direction": safe_float(row['wind_direction']),
                    "wind_speed": safe_float(row['wind_speed'])
                }

                # Gửi lên Kafka
                future = producer.send(TOPIC_NAME, key=message['City'], value=message)
                
                try:
                    record_metadata = future.get(timeout=10)
                    count += 1
                    print(f"[{count}] ✅ Đã gửi: {message['City']} | Temp: {message['temperature']} | Offset: {record_metadata.offset}")
                
                except KafkaError as e:
                    print(f"❌ Gửi thất bại dòng {count}: {e}")
                    break

                time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n🛑 Đã dừng Producer.")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        producer.close()
        print("🔌 Đã đóng kết nối.")

if __name__ == "__main__":
    run_producer()