import json
import time
import os
import threading
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# --- CẤU HÌNH CHUNG ---
BOOTSTRAP_SERVERS = ['localhost:9092']
HDFS_URL = 'http://localhost:30070' # Nhớ port-forward trước khi chạy
HDFS_USER = 'root'

# =============================================================================
# TẦNG 1: BASE CONSUMER (LỚP CHA CAO NHẤT)
# Nhiệm vụ: Kết nối Kafka, quản lý vòng lặp, xử lý lỗi mạng.
# =============================================================================
class BaseConsumer(threading.Thread):
    def __init__(self, topic, group_id, batch_size=1):
        threading.Thread.__init__(self)
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.buffer = []
        self.stop_event = threading.Event()
        self.consumer = None

    def _connect(self):
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"✅ [{self.__class__.__name__}] Đã nối Kafka topic '{self.topic}' (Group: {self.group_id})")
        except Exception as e:
            print(f"❌ [{self.__class__.__name__}] Lỗi kết nối Kafka: {e}")

    def process_record(self, record):
        """Hàm này xử lý từng bản ghi lẻ (Dành cho Streaming)"""
        pass

    def process_batch(self, batch):
        """Hàm này xử lý cả lô bản ghi (Dành cho HDFS)"""
        pass

    def run(self):
        self._connect()
        if not self.consumer: return

        print(f"🚀 [{self.__class__.__name__}] Bắt đầu chạy...")
        
        while not self.stop_event.is_set():
            msg_pack = self.consumer.poll(timeout_ms=1000)
            
            for tp, messages in msg_pack.items():
                for msg in messages:
                    data = msg.value
                    
                    # Cách 1: Xử lý từng dòng (cho Streaming)
                    if self.batch_size == 1:
                        self.process_record(data)
                    
                    # Cách 2: Gom batch (cho HDFS)
                    else:
                        self.buffer.append(data)
                        if len(self.buffer) >= self.batch_size:
                            self.process_batch(self.buffer)
                            self.buffer = [] # Reset rổ
        
        self.consumer.close()
        print(f"🛑 [{self.__class__.__name__}] Đã dừng.")

    def stop(self):
        self.stop_event.set()


# =============================================================================
# TẦNG 2: NHÓM CONSUMER THEO CHỨC NĂNG
# =============================================================================

# --- NHÓM 1: HDFS ARCHIVER (Batch Layer) ---
class HDFSConsumerBase(BaseConsumer):
    def __init__(self, topic, hdfs_folder):
        # HDFS cần gom batch lớn (ví dụ 50 tin) mới ghi để tối ưu
        super().__init__(topic, group_id='hdfs-archiver-group', batch_size=50)
        self.hdfs_folder = hdfs_folder
        self.client = self._connect_hdfs()

    def _connect_hdfs(self):
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            client.makedirs(self.hdfs_folder) # Tạo folder nếu chưa có
            return client
        except Exception as e:
            print(f"⚠️ Không nối được HDFS: {e}")
            return None

    def process_batch(self, batch):
        if not self.client: return
        
        # Tạo tên file: topic_timestamp.json
        filename = f"{self.topic}_{int(time.time())}.json"
        full_path = os.path.join(self.hdfs_folder, filename)
        
        try:
            # Ghi file JSON Lines
            content = "\n".join([json.dumps(r) for r in batch])
            with self.client.write(full_path, encoding='utf-8') as writer:
                writer.write(content)
            print(f"💾 [{self.__class__.__name__}] Đã ghi {len(batch)} dòng vào {full_path}")
        except Exception as e:
            print(f"❌ Lỗi ghi HDFS: {e}")

# --- NHÓM 2: SPEED LAYER (Giả lập Spark Streaming bằng Python) ---
# Lưu ý: Trong thực tế Production, lớp này sẽ là code Spark Scala/PySpark riêng biệt.
# Nhưng ở đây ta viết class Python để demo logic xử lý luồng theo yêu cầu OOP của bạn.
class SpeedLayerConsumerBase(BaseConsumer):
    def __init__(self, topic):
        # Speed Layer cần xử lý ngay lập tức (Batch size = 1)
        super().__init__(topic, group_id='spark-streaming-group', batch_size=1)

    def process_record(self, data):
        raise NotImplementedError("Class con phải tự định nghĩa logic cảnh báo!")


# =============================================================================
# TẦNG 3: CÁC CONSUMER CỤ THỂ (IMPLEMENTATION)
# =============================================================================

# --- CỤM HDFS CONSUMERS ---
class WeatherHDFSConsumer(HDFSConsumerBase):
    def __init__(self):
        super().__init__(topic='nyc-weather-raw', hdfs_folder='/nyc_data/weather')

class NYC311HDFSConsumer(HDFSConsumerBase):
    def __init__(self):
        super().__init__(topic='nyc-311-data', hdfs_folder='/nyc_data/311')

class TaxiHDFSConsumer(HDFSConsumerBase):
    def __init__(self):
        super().__init__(topic='nyc-taxi-data', hdfs_folder='/nyc_data/taxi')

class CollisionHDFSConsumer(HDFSConsumerBase):
    def __init__(self):
        super().__init__(topic='nyc-collision-data', hdfs_folder='/nyc_data/collision')


# --- CỤM SPEED LAYER CONSUMERS ---
class WeatherAlertConsumer(SpeedLayerConsumerBase):
    def __init__(self):
        super().__init__(topic='nyc-weather-raw')
    
    def process_record(self, data):
        # Logic nghiệp vụ: Cảnh báo nhiệt độ
        temp = data.get('temperature', 0)
        if temp > 30:
            print(f"🔥 [ALERT-WEATHER] Nóng quá ({temp}°C) tại {data.get('location')}")

class TaxiRealtimeConsumer(SpeedLayerConsumerBase):
    def __init__(self):
        super().__init__(topic='nyc-taxi-data')

    def process_record(self, data):
        # Logic nghiệp vụ: Theo dõi doanh thu taxi
        amount = data.get('total_amount', 0)
        if amount > 80:
            print(f"🚕 [ALERT-TAXI] Chuyến đi VIP giá cao: ${amount}")

# ... Bạn có thể viết thêm CollisionAlertConsumer, v.v...


# =============================================================================
# MAIN: CHẠY TOÀN BỘ HỆ THỐNG
# =============================================================================
if __name__ == "__main__":
    print("--- KHỞI ĐỘNG HỆ THỐNG CONSUMER OOP ---")
    
    # 1. Khởi tạo danh sách các Consumer muốn chạy
    consumers = [
        # Nhóm HDFS (Lưu trữ)
        WeatherHDFSConsumer(),
        # NYC311HDFSConsumer(), # Bỏ comment nếu muốn chạy
        # TaxiHDFSConsumer(),
        
        # Nhóm Speed Layer (Cảnh báo)
        WeatherAlertConsumer(),
        # TaxiRealtimeConsumer()
    ]

    # 2. Bắt đầu chạy tất cả các luồng
    try:
        for c in consumers:
            c.start()
        
        # Giữ chương trình chạy
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nĐang dừng hệ thống...")
        for c in consumers:
            c.stop()
        for c in consumers:
            c.join()
        print("✅ Đã tắt.")