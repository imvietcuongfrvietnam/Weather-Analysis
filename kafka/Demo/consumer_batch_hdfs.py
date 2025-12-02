import json
from kafka import KafkaConsumer

TOPIC_NAME = 'weather-data'
GROUP_ID = 'batch-hdfs-group' # <--- Tên Group quan trọng

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=GROUP_ID, # Định danh nhóm
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"--- Đang chạy Consumer Batch (Lưu HDFS) - Group: {GROUP_ID} ---")

for message in consumer:
    data = message.value
    # Ở đây code thật sẽ là lệnh ghi vào HDFS
    print(f"[HDFS Writer] Đã nhận: {data['city']} - Temp: {data['temperature']} -> Đang ghi file .parquet vào HDFS...")