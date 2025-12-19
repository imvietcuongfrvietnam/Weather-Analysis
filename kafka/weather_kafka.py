import time
import json
import csv
import os
from kafka import KafkaProducer

# --- CẤU HÌNH ---
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'nyc-weather-csv'
DATA_FILE = 'data/data_weather.csv' # Đường dẫn file của bạn
DELAY_SECONDS = 30  # Cứ 30 giây gửi 1 dòng (theo yêu cầu của bạn)

# --- KHỞI TẠO PRODUCER ---
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Chuyển dict -> json -> bytes
    key_serializer=lambda k: k.encode('utf-8') if k else None  # Chuyển key -> bytes
)

def safe_float(value):
    """Hàm phụ trợ: Chuyển string sang float, nếu lỗi hoặc rỗng thì trả về None"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def run_producer():
    print(f"🚀 BẮT ĐẦU ĐỌC FILE {DATA_FILE} VÀ GỬI VÀO KAFKA...")
    
    if not os.path.exists(DATA_FILE):
        print(f"❌ Lỗi: Không tìm thấy file tại {DATA_FILE}")
        return

    try:
        with open(DATA_FILE, mode='r', encoding='utf-8') as f:
            # Sử dụng DictReader để tự động map header thành key (datetime, City, temperature...)
            reader = csv.DictReader(f)
            
            count = 0
            for row in reader:
                # 1. Chuẩn hóa dữ liệu (CSV đọc ra toàn là string, cần chuyển về số)
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

                # 2. Gửi vào Kafka
                # Dùng tên thành phố làm Key để Kafka phân phối partition hợp lý
                producer.send(TOPIC_NAME, key=message['City'], value=message)
                
                count += 1
                print(f"[{count}] 📤 Gửi {message['City']} ({message['datetime']}): {message['temperature']}°C")

                # 3. Nghỉ 30 giây theo yêu cầu
                time.sleep(0.2)

    except KeyboardInterrupt:
        print("\n🛑 Đã dừng thủ công.")
    except Exception as e:
        print(f"❌ Có lỗi xảy ra: {e}")
    finally:
        producer.close()
        print("✅ Đã đóng kết nối Kafka.")

if __name__ == "__main__":
    run_producer()