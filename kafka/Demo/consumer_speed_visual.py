import json
from kafka import KafkaConsumer

TOPIC_NAME = 'weather-data'
GROUP_ID = 'speed-realtime-group' # <--- Tên Group KHÁC với nhóm Batch

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"--- Đang chạy Consumer Realtime (Spark Streaming) - Group: {GROUP_ID} ---")

for message in consumer:
    data = message.value
    # Ở đây code thật sẽ là đẩy vào Elasticsearch/Kibana
    if data['temperature'] > 35:
        alert = "!!! CẢNH BÁO NÓNG !!!"
    else:
        alert = ""
    
    print(f"[Realtime View] {data['timestamp']} | {data['city']} | {data['temperature']}°C {alert}")