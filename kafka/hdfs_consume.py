import json
import time
import os
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient # Thư viện giao tiếp HDFS qua Web

# --- CẤU HÌNH ---
KAFKA_HOST = 'localhost:9092'
TOPIC_NAME = 'nyc-weather-raw' # Tên topic bạn đã tạo
HDFS_URL = 'http://localhost:9870' # WebHDFS Port (đã port-forward)
HDFS_USER = 'root' # User mặc định của image bde2020
HDFS_OUTPUT_DIR = '/nyc_data/weather' # Thư mục trên HDFS

# Cấu hình Batch (Gom nhóm)
BATCH_SIZE = 50  # Cứ 50 tin nhắn thì ghi 1 file (Demo để thấp cho nhanh thấy)

def get_hdfs_client():
    """Kết nối tới HDFS"""
    try:
        # InsecureClient dùng cho cụm không có Kerberos (mặc định)
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        return client
    except Exception as e:
        print(f"❌ Không kết nối được HDFS: {e}")
        return None

def run_consumer():
    # 1. Kết nối Kafka
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_HOST,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='hdfs-writer-group'
    )
    print(f"🎧 Đang lắng nghe Kafka topic: {TOPIC_NAME}...")

    # 2. Kết nối HDFS
    hdfs_client = get_hdfs_client()
    if not hdfs_client: return

    # Tạo thư mục trên HDFS nếu chưa có
    try:
        hdfs_client.makedirs(HDFS_OUTPUT_DIR)
        print(f"📂 Đã đảm bảo thư mục tồn tại: {HDFS_OUTPUT_DIR}")
    except:
        pass

    buffer = [] # Rổ chứa tin nhắn tạm

    for message in consumer:
        data = message.value
        buffer.append(data)
        
        # In dấu chấm để biết đang chạy
        print(".", end="", flush=True)

        # 3. Kiểm tra nếu rổ đầy thì ghi xuống HDFS
        if len(buffer) >= BATCH_SIZE:
            print(f"\n📦 Đủ {BATCH_SIZE} tin nhắn. Đang ghi xuống HDFS...")
            
            # Tạo tên file duy nhất theo thời gian
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"weather_{timestamp}.json"
            hdfs_path = f"{HDFS_OUTPUT_DIR}/{filename}"

            try:
                # Chuyển list dữ liệu thành chuỗi JSON (mỗi dòng 1 record)
                # Dạng này gọi là JSON Lines, rất tốt cho Spark đọc sau này
                file_content = "\n".join([json.dumps(record) for record in buffer])
                
                # Ghi vào HDFS
                # encoding='utf-8' quan trọng để ghi text
                with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
                    writer.write(file_content)
                
                print(f"✅ Đã ghi file: {hdfs_path}")
                
                # Xóa rổ để gom đợt mới
                buffer = []
                
            except Exception as e:
                print(f"❌ Lỗi ghi file HDFS: {e}")
                # Lưu ý: Nếu lỗi thực tế nên có cơ chế retry, ở đây skip để demo

if __name__ == "__main__":
    # Chờ 1 chút để port-forward ổn định
    print("⏳ Vui lòng đảm bảo bạn đã chạy 'kubectl port-forward svc/namenode 9870:9870'")
    time.sleep(2)
    run_consumer()