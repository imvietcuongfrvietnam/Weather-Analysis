import json
import time
import os
import threading
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# --- CẤU HÌNH CHUNG ---
BOOTSTRAP_SERVERS = ['localhost:9092']
HDFS_URL = 'http://localhost:9870' # Nhớ port-forward trước khi chạy
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