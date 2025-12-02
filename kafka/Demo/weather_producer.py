import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Cấu hình Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8') # Key phải là bytes
)

TOPIC_NAME = 'weather-data'
CITIES = ["Hanoi", "HoChiMinh", "DaNang", "HaiPhong", "CanTho"]

def generate_weather_data():
    city = random.choice(CITIES)
    return {
        "city": city,
        "temperature": round(random.uniform(10, 40), 1), # Nhiệt độ 10-40 độ
        "humidity": random.randint(30, 90),              # Độ ẩm 30-90%
        "wind_speed": round(random.uniform(0, 20), 1),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }, city # Trả về cả city để làm Partition Key

print("--- Bắt đầu gửi dữ liệu thời tiết ---")

try:
    while True:
        data, city_key = generate_weather_data()
        
        # Gửi vào Kafka:
        # key=city_key: Đảm bảo cùng 1 thành phố luôn vào cùng 1 Partition
        future = producer.send(TOPIC_NAME, value=data, key=city_key)
        
        # Lấy metadata để demo xem nó rơi vào Partition nào
        record_metadata = future.get(timeout=10)
        
        print(f"[Producer] Gửi {data['city']} -> Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")
        
        time.sleep(2) # Giả lập 2 giây có 1 bản tin mới
except KeyboardInterrupt:
    print("Dừng Producer.")
    producer.close()