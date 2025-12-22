from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType
import sys
import os

try:
    # Import trực tiếp không qua folder 'config'
    import kafka_config
    import data_schemas
    weather_schema = data_schemas.weather_schema
    print("✅ DataReader: Loaded kafka_config and weather_schema")
except ImportError as e:
    print(f"❌ DataReader Import Error: {e}")
    sys.exit(1)

class DataReader:
    def __init__(self, spark: SparkSession, source_type: str = "kafka", kafka_mode: str = "streaming"):
        self.spark = spark
        self.source_type = source_type
        self.kafka_mode = kafka_mode
        print(f"📊 DataReader initialized | Source: {source_type.upper()}")        
    
    def read_weather_data(self) -> DataFrame:
        if self.source_type == "kafka":
            # kafka_config giờ đã được import ở trên nên sẽ không bị NameError
            topic = kafka_config.KAFKA_TOPICS.get('weather')
            return self._read_from_kafka(topic, weather_schema)
            
        elif self.source_type == "file":
            return self._read_from_file("weather_data.csv", weather_schema)
            
        else:
            raise ValueError(f"❌ Unknown source type: {self.source_type}")

    def _read_from_kafka(self, topic: str, schema: StructType) -> DataFrame:
        print(f"📡 Connecting to Kafka: {kafka_config.KAFKA_BOOTSTRAP_SERVERS}")
        
        if self.kafka_mode == "streaming":
            df_reader = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false")
        else:
            df_reader = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", topic)
        
        df = df_reader.load()
        
        # Parse Binary -> JSON Struct
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*")
            
        print("✅ Kafka Stream initialized successfully.")
        return parsed_df

    def _read_from_file(self, filename: str, schema: StructType) -> DataFrame:
        file_path = os.path.join(self.local_data_path, filename)
        if filename.endswith(".csv"):
            return self.spark.read.csv(file_path, header=True, schema=schema)
        elif filename.endswith(".json"):
            return self.spark.read.json(file_path, schema=schema)
        else:
            return self.spark.read.parquet(file_path)