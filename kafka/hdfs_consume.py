import json
import time
import os
import threading
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import BaseConsumer
# --- CẤU HÌNH CHUNG ---
BOOTSTRAP_SERVERS = ['localhost:9092']
HDFS_URL = 'http://localhost:30070' # Nhớ port-forward trước khi chạy
HDFS_USER = 'root'
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