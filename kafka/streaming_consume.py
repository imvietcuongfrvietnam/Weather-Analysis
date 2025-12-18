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
HDFS_URL = 'http://localhost:9870' # Nhớ port-forward trước khi chạy
HDFS_USER = 'root'
# --- NHÓM 2: SPEED LAYER (Giả lập Spark Streaming bằng Python) ---
# Lưu ý: Trong thực tế Production, lớp này sẽ là code Spark Scala/PySpark riêng biệt.
# Nhưng ở đây ta viết class Python để demo logic xử lý luồng theo yêu cầu OOP của bạn.
class SpeedLayerConsumerBase(BaseConsumer):
    def __init__(self, topic):
        # Speed Layer cần xử lý ngay lập tức (Batch size = 1)
        super().__init__(topic, group_id='spark-streaming-group', batch_size=1)

    def process_record(self, data):
        raise NotImplementedError("Class con phải tự định nghĩa logic cảnh báo!")
        #define something more