import json
import time
import os
import threading
import subprocess
from datetime import datetime
from kafka import KafkaConsumer

# --- CẤU HÌNH ---
BOOTSTRAP_SERVERS = ['localhost:9092']

# 👇 [QUAN TRỌNG] Thay tên này bằng tên pod thật của bạn (lấy từ 'kubectl get pods')
NAMENODE_POD = "namenode-749c6d6bf7-jgg9z"  # Ví dụ lấy từ ảnh bạn gửi

# =============================================================================
# 1. BASE CLASS (Giữ nguyên)
# =============================================================================
class BaseConsumer(threading.Thread):
    def __init__(self, topic, group_id):
        threading.Thread.__init__(self)
        self.topic = topic
        self.group_id = group_id
        self.stop_event = threading.Event()
        self.consumer = None

    def _connect_kafka(self):
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"✅ [{self.__class__.__name__}] Đã nối Kafka topic '{self.topic}'")
        except Exception as e:
            print(f"❌ [{self.__class__.__name__}] Lỗi kết nối Kafka: {e}")

    def process_message(self, data):
        raise NotImplementedError

    def run(self):
        self._connect_kafka()
        if not self.consumer: return
        print(f"🚀 [{self.__class__.__name__}] Bắt đầu chạy...")
        while not self.stop_event.is_set():
            msg_pack = self.consumer.poll(timeout_ms=1000)
            for tp, messages in msg_pack.items():
                for msg in messages:
                    self.process_message(msg.value)
        self.consumer.close()
        print(f"🛑 [{self.__class__.__name__}] Đã dừng.")

    def stop(self):
        self.stop_event.set()

# =============================================================================
# 2. HDFS CONSUMER (PHIÊN BẢN SỬ DỤNG KUBECTL - FIX LỖI DATANODE)
# =============================================================================
class HDFSConsumerBase(BaseConsumer):
    def __init__(self, topic, hdfs_folder, batch_size=50):
        super().__init__(topic, group_id='hdfs-archiver-group')
        self.hdfs_folder = hdfs_folder
        self.batch_size = batch_size
        self.buffer = [] 
        
        # Tạo thư mục trên HDFS ngay khi khởi động
        self._ensure_hdfs_dir()

    def _ensure_hdfs_dir(self):
        """Dùng kubectl để ra lệnh cho Namenode tạo thư mục"""
        try:
            cmd = f"kubectl exec {NAMENODE_POD} -- hdfs dfs -mkdir -p {self.hdfs_folder}"
            subprocess.run(cmd, shell=True, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"📂 Đã đảm bảo thư mục HDFS: {self.hdfs_folder}")
        except Exception as e:
            print(f"⚠️ Cảnh báo tạo thư mục: {e}")

    def flush_buffer(self):
        if not self.buffer: return

        # 1. Tạo file tạm trên máy tính của bạn (Localhost)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data_{timestamp}.json"
        local_path = f"/tmp/{filename}" # Lưu vào thư mục tạm của Linux
        
        try:
            # Ghi dữ liệu vào file tạm ở máy thật
            with open(local_path, 'w', encoding='utf-8') as f:
                for record in self.buffer:
                    f.write(json.dumps(record) + "\n")
            
            # 2. Copy file từ máy thật vào bên trong Pod Namenode
            pod_tmp_path = f"/tmp/{filename}"
            # Lệnh: kubectl cp /tmp/local.json namenode:/tmp/remote.json
            cp_cmd = f"kubectl cp {local_path} {NAMENODE_POD}:{pod_tmp_path}"
            subprocess.check_call(cp_cmd, shell=True, stdout=subprocess.DEVNULL)

            # 3. Ra lệnh cho Namenode đưa file vào HDFS chính thức
            # Lệnh: hdfs dfs -moveFromLocal ...
            hdfs_cmd = f"kubectl exec {NAMENODE_POD} -- hdfs dfs -moveFromLocal {pod_tmp_path} {self.hdfs_folder}/{filename}"
            subprocess.check_call(hdfs_cmd, shell=True, stdout=subprocess.DEVNULL)

            print(f"💾 [{self.__class__.__name__}] Đã ghi {len(self.buffer)} dòng vào HDFS: {self.hdfs_folder}/{filename}")
            
            # Dọn dẹp file rác
            os.remove(local_path)
            self.buffer = [] 

        except Exception as e:
            print(f"❌ Lỗi ghi HDFS qua Kubectl: {e}")

    def process_message(self, data):
        self.buffer.append(data)
        if len(self.buffer) >= self.batch_size:
            self.flush_buffer()

# =============================================================================
# 3. STREAMING & IMPLEMENTATION (Giữ nguyên logic cũ)
# =============================================================================
class SparkStreamingSimBase(BaseConsumer):
    def __init__(self, topic):
        super().__init__(topic, group_id='spark-streaming-group')
    def process_message(self, data):
        self.process_logic(data)
    def process_logic(self, data): pass

class WeatherHDFSConsume(HDFSConsumerBase):
    def __init__(self):
        super().__init__(topic='nyc-weather-raw', hdfs_folder='/nyc_data/weather')

class WeatherStreamingConsume(SparkStreamingSimBase):
    def __init__(self):
        super().__init__(topic='nyc-weather-raw')
    def process_logic(self, data):
        if data.get('temperature', 0) > 30:
            print(f"🔥 [ALERT-WEATHER] Nóng quá ({data.get('temperature')}°C)")

# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    print(f"--- CONSUMER SYSTEM (Using Pod: {NAMENODE_POD}) ---")
    
    # Danh sách worker
    workers = [
        WeatherHDFSConsume(),
        WeatherStreamingConsume()
    ]

    try:
        for w in workers: w.start()
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\nĐang dừng...")
        for w in workers: w.stop()
        for w in workers: w.join()