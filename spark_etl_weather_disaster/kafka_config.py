# ==========================================
# CẤU HÌNH KAFKA (CHỈ CHO WEATHER)
# ==========================================

# 1. KẾT NỐI SERVER
# Địa chỉ Kafka (dùng localhost vì bạn đang chạy Docker/Local)
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# 2. CẤU HÌNH TOPIC
# Key: Tên dùng trong code | Value: Tên thực tế trên Kafka
# LƯU Ý: Value phải khớp Y HỆT với 'TOPIC_NAME' bên file Producer
KAFKA_TOPICS = {
    "weather": "weather" 
}

# 3. CẤU HÌNH CONSUMER GROUP
# Định danh cho nhóm đọc dữ liệu này
KAFKA_GROUP_ID = "spark-weather-consumer-group-v1"

# 4. CẤU HÌNH OFFSET (QUAN TRỌNG)
# "earliest": Đọc từ tin nhắn cũ nhất có trong Kafka (Khuyên dùng khi Dev/Test)
# "latest": Chỉ đọc tin nhắn mới vừa được gửi đến (Dùng cho Production)
KAFKA_STARTING_OFFSET = "earliest"

# 5. CẤU HÌNH SPARK READSTREAM
# Bộ config chuẩn để ném thẳng vào hàm spark.readStream.format("kafka").options(**config)
SPARK_KAFKA_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    
    # --- CHỈ ĐỊNH RÕ TOPIC CẦN ĐỌC Ở ĐÂY ---
    "subscribe": KAFKA_TOPICS["weather"], 
    
    "startingOffsets": KAFKA_STARTING_OFFSET,
    "failOnDataLoss": "false",     # False để tránh lỗi crash nếu Kafka xoá bớt log cũ
    "maxOffsetsPerTrigger": 1000,  # Giới hạn số dòng xử lý mỗi lần (tránh tràn RAM)
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