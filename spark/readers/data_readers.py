from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
# Đảm bảo bạn đã có file schemas/data_schemas.py
from schemas.data_schemas import * from datetime import datetime, timedelta
import random

# ========================================
# 1. FAKE DATA READER (Dùng để test logic transform)
# Lưu ý: Cái này trả về Batch DataFrame -> Chỉ dùng để debug, không chạy được .writeStream
# ========================================
class FakeDataReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_weather_data(self, num_records=1000):
        print(f"📊 [FAKE] Generating {num_records} weather records...")
        
        data = []
        base_date = datetime(2016, 1, 1)
        
        for i in range(num_records):
            record = {
                "datetime": base_date + timedelta(hours=i),
                "city": "New York",
                "temperature": random.uniform(260, 310),
                "humidity": random.uniform(30, 95),
                "pressure": random.uniform(1000, 1025),
                "wind_speed": random.uniform(0, 15),
                "wind_direction": random.uniform(0, 360),
                "weather_description": random.choice(["clear sky", "few clouds", "rain", "snow", "thunderstorm"]),
                "rain_1h": random.uniform(0, 5) if random.random() > 0.7 else 0.0,
                "snow_1h": random.uniform(0, 3) if random.random() > 0.9 else 0.0,
                "clouds_all": random.randint(0, 100),
            }
            data.append(record)
        
        # Lưu ý: Đây là Static DataFrame
        df = self.spark.createDataFrame(data, schema=weather_schema)
        print(f"   ✅ Generated {df.count()} weather records (BATCH MODE)")
        return df
    

# ========================================
# 2. REAL DATA READER (KAFKA) - QUAN TRỌNG
# ========================================

class RealDataReader:
    def __init__(self, spark: SparkSession, source_type="kafka", kafka_mode="streaming"):
        self.spark = spark
        self.kafka_mode = kafka_mode  # 'streaming' hoặc 'batch'
    
    def read_weather_data(self, topic="weather_data", bootstrap_servers="kafka:9092"):
        """
        Tự động chuyển đổi giữa readStream và read thường
        """
        print(f"🎧 Reading from Kafka topic: {topic} | Mode: {self.kafka_mode}")

        # Cấu hình Kafka cơ bản
        options = {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": "earliest", # Đọc từ đầu nếu chưa có checkpoint
            "failOnDataLoss": "false"
        }

        if self.kafka_mode == "streaming":
            # --- STREAMING MODE (Cho main_etl.py) ---
            df = self.spark.readStream \
                .format("kafka") \
                .options(**options) \
                .load()
        else:
            # --- BATCH MODE (Cho debugging/testing) ---
            df = self.spark.read \
                .format("kafka") \
                .options(**options) \
                .load()

        # Kafka trả về format binary (key, value), cần parse JSON -> Struct
        # Giả sử dữ liệu trong Kafka là JSON string
        # Bạn cần cast column 'value' từ binary sang string rồi parse json
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
                      .select(from_json(col("json_string"), weather_schema).alias("data")) \
                      .select("data.*") # Flatten struct ra các cột

        return parsed_df

# ========================================
# 3. WRAPPER CLASS (Để khớp với main_etl.py)
# ========================================
class DataReader:
    """
    Class này giúp main_etl.py gọi gọn gàng:
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    """
    def __init__(self, spark, source_type="kafka", kafka_mode="streaming"):
        if source_type == "fake":
            self.reader = FakeDataReader(spark)
        else:
            self.reader = RealDataReader(spark, source_type, kafka_mode)

    def read_weather_data(self):
        return self.reader.read_weather_data()