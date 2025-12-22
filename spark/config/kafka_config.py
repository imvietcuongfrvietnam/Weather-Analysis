# ==========================================
# CẤU HÌNH KAFKA (CHỈ CHO WEATHER)
# ==========================================

import os

# Ưu tiên lấy từ biến môi trường, nếu không có thì dùng localhost:9094 (cho local)
# Trong Kubernetes ta sẽ set biến này thành: weather-kafka:9092
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS",     "weather-kafka.default.svc.cluster.local:9092"
)

KAFKA_TOPICS = {
    "weather": "weather" 
}
KAFKA_GROUP_ID = "spark-weather-consumer-group-v1"
KAFKA_STARTING_OFFSET = "earliest"

SPARK_KAFKA_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "subscribe": KAFKA_TOPICS["weather"], 
    "startingOffsets": KAFKA_STARTING_OFFSET,
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": 1000,
}

# ===========================
# HÀM KIỂM TRA (HELPER)
# ===========================

def print_config():
    """In cấu hình ra màn hình để kiểm tra trước khi chạy"""
    print("\n" + "="*50)
    print("🚀 SPARK STREAMING KAFKA CONFIG")
    print("="*50)
    print(f"Server:     {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic:      {KAFKA_TOPICS['weather']}")
    print(f"Offset:     {KAFKA_STARTING_OFFSET}")
    print(f"Group ID:   {KAFKA_GROUP_ID}")
    print("="*50 + "\n")

if __name__ == "__main__":
    # Khi chạy trực tiếp file này, nó sẽ in cấu hình ra để bạn check
    print_config()