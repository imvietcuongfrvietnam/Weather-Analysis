"""
Kafka Configuration
Cấu hình kết nối Kafka cho Spark Streaming

HƯỚNG DẪN:
Nhận thông tin này từ người setup Kafka server:
1. KAFKA_BOOTSTRAP_SERVERS - Địa chỉ Kafka brokers
2. KAFKA_TOPICS - Tên các topics đã được tạo sẵn
3. KAFKA_GROUP_ID - Consumer group ID
"""

# ===========================
# KAFKA SERVER CONFIGURATION
# ===========================

# Kafka Bootstrap Servers (nhận từ người setup Kafka)
# Format: "host1:9092,host2:9092,host3:9092"
# Local testing: "localhost:9092"
# Production: Địa chỉ Kafka cluster thật
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# ===========================
# KAFKA TOPICS
# ===========================
# Các topics cần subscribe (nhận từ người setup Kafka)

KAFKA_TOPICS = {
    "weather": "nyc-weather-raw",       # Topic cho dữ liệu weather
    "311": "nyc-311-data",              # Topic cho 311 requests
    "taxi": "nyc-taxi-data",            # Topic cho taxi trips
    "collision": "nyc-collision-data"   # Topic cho collisions
}

# ===========================
# CONSUMER CONFIGURATION
# ===========================

# Consumer Group ID - để Kafka track offset
KAFKA_GROUP_ID = "spark-weather-etl-consumer"

# Starting offset khi chạy lần đầu
# "earliest" - đọc từ đầu
# "latest" - chỉ đọc message mới
KAFKA_STARTING_OFFSET = "latest"

# ===========================
# ADVANCED SETTINGS (Optional)
# ===========================

# Max poll records
KAFKA_MAX_POLL_RECORDS = 500

# Session timeout (ms)
KAFKA_SESSION_TIMEOUT_MS = 30000

# Enable auto commit offset
KAFKA_ENABLE_AUTO_COMMIT = True

# ===========================
# SPARK KAFKA CONFIGURATION
# ===========================
# Các config này sẽ được thêm vào Spark readStream

SPARK_KAFKA_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "subscribe": None,  # Sẽ set động theo topic
    "startingOffsets": KAFKA_STARTING_OFFSET,
    "failOnDataLoss": "false",  # Không fail nếu data loss (cho development)
    "maxOffsetsPerTrigger": 1000,  # Giới hạn số records mỗi batch
}

# ===========================
# HELPER FUNCTIONS
# ===========================

def get_kafka_topic(data_type: str) -> str:
    """
    Lấy topic name theo loại dữ liệu
    
    Args:
        data_type: "weather", "311", "taxi", "collision"
    
    Returns:
        str: Topic name
    """
    return KAFKA_TOPICS.get(data_type, f"unknown-{data_type}")


def print_config():
    """In ra cấu hình hiện tại (để debug)"""
    print("\n" + "="*80)
    print("📡 KAFKA CONFIGURATION")
    print("="*80)
    print(f"Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Group ID:          {KAFKA_GROUP_ID}")
    print(f"Starting Offset:   {KAFKA_STARTING_OFFSET}")
    print(f"\nTopics:")
    for key, topic in KAFKA_TOPICS.items():
        print(f"  - {key:12}: {topic}")
    print("="*80 + "\n")


if __name__ == "__main__":
    # Test configuration
    print_config()
    
    # Test topic retrieval
    print("Example topic paths:")
    for data_type in ["weather", "311", "taxi", "collision"]:
        print(f"  {data_type}: {get_kafka_topic(data_type)}")


def validate_config(test_connection: bool = False):
    """
    Validate Kafka configuration
    
    Args:
        test_connection: If True, test actual connection to Kafka server
        
    Returns:
        bool: True if valid, raises ValueError if invalid
    """
    print("\n🔍 Validating Kafka Configuration...")
    
    # Check required fields
    if not KAFKA_BOOTSTRAP_SERVERS:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is not set!")
    
    if not KAFKA_GROUP_ID:
        raise ValueError("KAFKA_GROUP_ID is not set!")
    
    if not KAFKA_TOPICS:
        raise ValueError("KAFKA_TOPICS dictionary is empty!")
    
    # Warn if using default localhost
    if "localhost" in KAFKA_BOOTSTRAP_SERVERS.lower():
        print("   ⚠️  WARNING: Using localhost Kafka server")
        print("      Make sure Kafka is running locally or update config for production")
    
    # Validate topic names
    for key, topic in KAFKA_TOPICS.items():
        if not topic:
            raise ValueError(f"Topic for '{key}' is empty!")
    
    print("   ✅ Configuration validation passed!")
    
    # Optional: Test actual connection
    if test_connection:
        try:
            from connection_utils import validate_kafka_connection
            if not validate_kafka_connection(KAFKA_BOOTSTRAP_SERVERS):
                print("   ⚠️  Connection test failed but continuing anyway")
        except ImportError:
            print("   💡 connection_utils not found, skipping connection test")
    
    print_config()
    return True
