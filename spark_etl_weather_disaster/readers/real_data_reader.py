"""
Real Data Readers
Read data from JSON files (for testing) or Kafka (for production)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType
from schemas.data_schemas import (
    weather_schema_long,
    service_311_schema,
    taxi_trip_schema_2016,
    collision_schema
)
import os

class DataReader:
    """
    Data reader with multiple sources:
    - JSON files (for testing with pre-generated data)
    - Kafka streams (for production)
    - HDFS (for batch processing)
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "json", kafka_mode: str = "batch"):
        """
        Args:
            spark: SparkSession
            source_type: "json", "kafka", or "hdfs"
            kafka_mode: "batch" or "streaming" (only used when source_type="kafka")
        """
        self.spark = spark
        self.source_type = source_type
        self.kafka_mode = kafka_mode  # batch or streaming
        self.data_dir = "./data"
        
        print(f"\n📊 DataReader initialized:")
        print(f"   Source: {source_type}")
        if source_type == "kafka":
            print(f"   Kafka Mode: {kafka_mode.upper()}")
    
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
        Read from Kafka (batch or streaming mode)
        Uses configuration from kafka_config.py
        Parses JSON messages according to the appropriate schema
        """
        try:
            from kafka_config import SPARK_KAFKA_CONFIG, print_config as print_kafka_config
            
            print(f"\n📡 Connecting to Kafka ({self.kafka_mode.upper()} mode)...")
            print_kafka_config()
            
            # Create config for this specific topic
            kafka_config = SPARK_KAFKA_CONFIG.copy()
            kafka_config["subscribe"] = topic
            
            print(f"   📥 Subscribing to topic: {topic}")
            print(f"   🔄 Mode: {self.kafka_mode.upper()}")
            
            # HYBRID MODE: Choose batch or streaming
            if self.kafka_mode == "streaming":
                # STREAMING MODE: Real-time processing
                print(f"   ⚡ Using Spark Structured Streaming (real-time)")
                df = self.spark.readStream.format("kafka")
            else:
                # BATCH MODE: Micro-batch processing (easier, compatible with current ETL)
                print(f"   📦 Using Batch mode (micro-batches)")
                df = self.spark.read.format("kafka")
            
            # Add all Kafka configurations
            for key, value in kafka_config.items():
                if value is not None:
                    df = df.option(key, value)
            
            df = df.load()
            
            # Kafka returns: key, value, topic, partition, offset, timestamp
            # We only need the value (JSON string)
            df = df.selectExpr("CAST(value AS STRING) as json_str")
            
            # PRODUCTION: Parse JSON based on schema
            # Determine schema based on topic name
            schema = self._get_schema_for_topic(topic)
            if schema:
                print(f"   🔧 Parsing JSON with schema: {topic}")
                df = df.select(from_json("json_str", schema).alias("data")) \
                       .select("data.*")
                print(f"   ✅ Schema applied successfully!")
            else:
                print(f"   ⚠️  No schema found for topic: {topic}")
                print(f"   💡 Returning raw JSON - add schema mapping in _get_schema_for_topic()")
            
            print(f"   ✅ Connected to Kafka successfully!")
            return df
            
        except ImportError:
            print(f"   ⚠️  kafka_config.py not found!")
            print(f"   💡 Please create kafka_config.py with Kafka server details")
            raise NotImplementedError("kafka_config.py not found. Cannot connect to Kafka.")
        except Exception as e:
            print(f"   ❌ Error connecting to Kafka: {e}")
            print(f"   💡 Check Kafka server is running and config is correct")
            raise
    
    def _get_schema_for_topic(self, topic: str) -> StructType:
        """
        Map Kafka topic name to appropriate Spark schema
        
        Args:
            topic: Kafka topic name
            
        Returns:
            StructType schema or None if not found
        """
        # Topic name mappings (customize based on your kafka_config.py)
        topic_schema_map = {
            # Common patterns
            "weather": weather_schema_long,
            "311": service_311_schema,
            "taxi": taxi_trip_schema_2016,
            "collision": collision_schema,
            # Specific topic names from kafka_config.py
            "nyc-weather-raw": weather_schema_long,
            "nyc-311-data": service_311_schema,
            "nyc-taxi-data": taxi_trip_schema_2016,
            "nyc-collision-data": collision_schema,
        }
        
        # Try exact match first
        if topic in topic_schema_map:
            return topic_schema_map[topic]
        
        # Try partial match (if topic contains keyword)
        for keyword, schema in topic_schema_map.items():
            if keyword.lower() in topic.lower():
                return schema
        
        # No schema found
        return None

    
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
