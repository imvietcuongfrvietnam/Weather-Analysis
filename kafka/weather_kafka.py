import time
import json
import csv
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
# --- THÊM IMPORT NÀY ---
from datetime import datetime 

# --- CẤU HÌNH ---
BOOTSTRAP_SERVERS = ['localhost:9094'] 
TOPIC_NAME = 'weather'
DATA_FILE = '../data/data_weather.csv' 
DELAY_SECONDS = 0.5 

# --- KHỞI TẠO PRODUCER ---
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        request_timeout_ms=10000 
    )
    print(f"✅ Đã kết nối tới Kafka tại: {BOOTSTRAP_SERVERS}")
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
    print(f"🕒 Chế độ: Giả lập Real-time (Thay đổi năm 2012 -> Hiện tại)")
    
    if not os.path.exists(DATA_FILE):
        print(f"❌ Lỗi: Không tìm thấy file {DATA_FILE}. Hãy chạy script preprocess trước!")
        return

    try:
        with open(DATA_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            count = 0
            for row in reader:
                # Lấy thời gian hiện tại để dashboard hiển thị được
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # 1. Chuẩn hóa dữ liệu
                message = {
                    # SỬA Ở ĐÂY: Dùng current_time thay vì row['datetime'] cũ
                    "datetime": current_time, 
                    "City": row['City'],
                    "temperature": safe_float(row['temperature']),
                    "humidity": safe_float(row['humidity']),
                    "pressure": safe_float(row['pressure']),
                    "weather_desc": row['weather_desc'],
                    "wind_direction": safe_float(row['wind_direction']),
                    "wind_speed": safe_float(row['wind_speed'])
                }

                # key là City để chia partition
                future = producer.send(TOPIC_NAME, key=message['City'], value=message)
                
                try:
                    record_metadata = future.get(timeout=10)
                    
                    count += 1
                    print(f"[{count}] ✅ Đã gửi: {message['datetime']} | {message['City']} | "
                          f"Temp: {message['temperature']} | Offset: {record_metadata.offset}")
                
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