"""
Real Data Readers
Read data from JSON files (for testing) or Kafka (for production)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from schemas.data_schemas import *
import os

class DataReader:
    """
    Data reader with multiple sources:
    - JSON files (for testing with pre-generated data)
    - Kafka streams (for production)
    - HDFS (for batch processing)
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "json"):
        """
        Args:
            spark: SparkSession
            source_type: "json", "kafka", or "hdfs"
        """
        self.spark = spark
        self.source_type = source_type
        self.data_dir = "./data"
    
    def read_weather_data(self) -> DataFrame:
        """
        Read weather data from configured source
        """
        print(f"📊 Reading weather data from {self.source_type}...")
        
        if self.source_type == "json":
            return self._read_from_json("weather_data.json", weather_schema_long)
        elif self.source_type == "kafka":
            return self._read_from_kafka("weather-topic")
        elif self.source_type == "hdfs":
            return self._read_from_hdfs("/data/weather")
        else:
            raise ValueError(f"Unknown source type: {self.source_type}")
    
    def read_311_requests(self) -> DataFrame:
        """
        Read 311 service requests from configured source
        """
        print(f"📊 Reading 311 requests from {self.source_type}...")
        
        if self.source_type == "json":
            return self._read_from_json("311_requests.json", service_311_schema)
        elif self.source_type == "kafka":
            return self._read_from_kafka("311-topic")
        elif self.source_type == "hdfs":
            return self._read_from_hdfs("/data/311")
        else:
            raise ValueError(f"Unknown source type: {self.source_type}")
    
    def read_taxi_trips(self) -> DataFrame:
        """
        Read taxi trips from configured source
        """
        print(f"📊 Reading taxi trips from {self.source_type}...")
        
        if self.source_type == "json":
            return self._read_from_json("taxi_trips.json", taxi_trip_schema_2016)
        elif self.source_type == "kafka":
            return self._read_from_kafka("taxi-topic")
        elif self.source_type == "hdfs":
            return self._read_from_hdfs("/data/taxi")
        else:
            raise ValueError(f"Unknown source type: {self.source_type}")
    
    def read_collisions(self) -> DataFrame:
        """
        Read collision data from configured source
        """
        print(f"📊 Reading collision data from {self.source_type}...")
        
        if self.source_type == "json":
            return self._read_from_json("collisions.json", collision_schema)
        elif self.source_type == "kafka":
            return self._read_from_kafka("collision-topic")
        elif self.source_type == "hdfs":
            return self._read_from_hdfs("/data/collisions")
        else:
            raise ValueError(f"Unknown source type: {self.source_type}")
    
    # ============================================
    # Private methods for different sources
    # ============================================
    
    def _read_from_json(self, filename: str, schema: StructType) -> DataFrame:
        """
        Read from CSV file (for testing)
        Changed to CSV for Windows compatibility (no Hadoop winutils needed)
        """
        # Change extension from .json to .csv
        filename = filename.replace('.json', '.csv')
        path = os.path.join(self.data_dir, filename)
        
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"\n❌ Data file not found: {path}\n"
                f"💡 Please run: py -3.11 generate_data.py\n"
                f"   This will generate test data files in ./data/ directory"
            )
        
        # Read CSV files (all columns as string to avoid type conversion issues)
        # Let the cleaning/normalization steps handle type conversion
        df = self.spark.read.csv(path, header=True, inferSchema=False)
        print(f"   ✅ Loaded {df.count()} records from {filename}")
        return df
    
    def _read_from_kafka(self, topic: str) -> DataFrame:
        """
        Read from Kafka stream (for production)
        TODO: Implement when Kafka is available
        """
        print(f"   ⚠️  Kafka reader not implemented yet!")
        print(f"   📝 Will read from topic: {topic}")
        print(f"   🔧 Configuration needed:")
        print(f"      - kafka.bootstrap.servers")
        print(f"      - subscribe: {topic}")
        
        # Example implementation (commented out):
        """
        df = self.spark.readStream \\
            .format("kafka") \\
            .option("kafka.bootstrap.servers", "localhost:9092") \\
            .option("subscribe", topic) \\
            .option("startingOffsets", "earliest") \\
            .load()
        
        # Parse JSON from Kafka value
        df = df.selectExpr("CAST(value AS STRING) as json_str") \\
            .select(from_json("json_str", schema).alias("data")) \\
            .select("data.*")
        
        return df
        """
        
        raise NotImplementedError("Kafka reader not implemented yet. Use 'json' source type for now.")
    
    def _read_from_hdfs(self, path: str) -> DataFrame:
        """
        Read from HDFS (for batch processing)
        TODO: Implement when HDFS is available
        """
        print(f"   ⚠️  HDFS reader not implemented yet!")
        print(f"   📝 Will read from path: {path}")
        print(f"   🔧 Configuration needed:")
        print(f"      - HDFS namenode URL")
        print(f"      - Authentication")
        
        # Example implementation (commented out):
        """
        df = self.spark.read \\
            .format("parquet") \\
            .load(f"hdfs://namenode:9000{path}")
        
        return df
        """
        
        raise NotImplementedError("HDFS reader not implemented yet. Use 'json' source type for now.")
