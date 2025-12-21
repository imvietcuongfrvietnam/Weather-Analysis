from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType
import sys
import os

# --- IMPORT CONFIG & SCHEMA ---
# Vì main_etl.py đã setup sys.path, ta có thể import trực tiếp
try:
    from config import kafka_config
    # Giả định bạn để schema ở spark/schemas/weather_schema.py
    # Nếu tên file khác, hãy sửa lại dòng này
    from schemas.weather_schema import weather_schema 
except ImportError as e:
    print(f"❌ Lỗi Import trong DataReader: {e}")
    # Fallback schema cơ bản nếu không import được (để tránh crash app ngay lập tức)
    from pyspark.sql.types import StringType, StructField
    weather_schema = StructType([StructField("error", StringType(), True)])

class DataReader:
    """
    Class đọc dữ liệu chuyên dụng cho Weather Project.
    Hỗ trợ đọc từ:
    1. Kafka (Production - Streaming)
    2. JSON/CSV Local (Testing)
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "kafka", kafka_mode: str = "streaming"):
        """
        Args:
            spark: SparkSession object
            source_type: "kafka" hoặc "file"
            kafka_mode: "streaming" hoặc "batch"
        """
        self.spark = spark
        self.source_type = source_type
        self.kafka_mode = kafka_mode
        
        # Đường dẫn data test cục bộ (nếu cần)
        self.local_data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
        
        print(f"📊 DataReader initialized | Source: {source_type.upper()} | Mode: {kafka_mode.upper()}")
    
    def read_weather_data(self) -> DataFrame:
        """
        Hàm chính được gọi bởi main_etl.py
        """
        if self.source_type == "kafka":
            # Lấy tên topic từ file config
            topic = kafka_config.KAFKA_TOPICS.get('weather', 'weather_data')
            return self._read_from_kafka(topic, weather_schema)
            
        elif self.source_type == "file":
            return self._read_from_file("weather_data.csv", weather_schema)
            
        else:
            raise ValueError(f"❌ Unknown source type: {self.source_type}")

    # ============================================
    # Private Methods
    # ============================================
    
    def _read_from_kafka(self, topic: str, schema: StructType) -> DataFrame:
        """
        Đọc dữ liệu từ Kafka và Parse JSON struct
        """
        print(f"📡 Connecting to Kafka servers: {kafka_config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"📥 Subscribing to topic: {topic}")

        # 1. Cấu hình Kafka Reader
        if self.kafka_mode == "streaming":
            df_reader = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false")
        else:
            # Batch mode (cho debug)
            df_reader = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", topic)
        
        # 2. Load dữ liệu Raw (Binary)
        df = df_reader.load()
        
        # 3. Parse Data: Binary -> String -> JSON Struct
        # Kafka trả về cột 'value' dạng binary
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*") # Flatten struct ra các cột
            
        print("✅ Kafka Stream initialized successfully.")
        return parsed_df

    def _read_from_file(self, filename: str, schema: StructType) -> DataFrame:
        """
        Đọc file CSV/JSON local để test logic mà không cần bật Kafka
        """
        file_path = os.path.join(self.local_data_path, filename)
        print(f"📂 Reading local file: {file_path}")
        
        if filename.endswith(".csv"):
            return self.spark.read.csv(file_path, header=True, schema=schema)
        elif filename.endswith(".json"):
            return self.spark.read.json(file_path, schema=schema)
        else:
            # Mặc định thử đọc parquet
            return self.spark.read.parquet(file_path)