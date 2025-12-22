from pyspark.sql import SparkSession
import sys
import os

# SETUP MÔI TRƯỜNG: Ưu tiên tìm config trong folder job hiện tại
if '/app/job' not in sys.path:
    sys.path.insert(0, '/app/job')
if '/app' not in sys.path:
    sys.path.append('/app')

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

try:
    import kafka_config
    import minio_config
    import data_schemas
    print("✅ Successfully imported all configs and schemas")
except ImportError as e:
    print(f"❌ Failed to import configs: {e}")
    sys.exit(1)

from readers.real_data_reader import DataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from writers.redis_data_writer import RedisWriter 
from writers.minio_writer import MinIOWriter

def create_spark_session():
    # Khai báo đủ 3 gói thư viện quan trọng
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherLambdaArchitecture") \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars.ivy", "/tmp/.ivy2") 
        
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_etl_pipeline():
    print("🚀 SPARK PIPELINE STARTING...")
    spark = create_spark_session()
    
    reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    minio_writer = MinIOWriter()
    redis_writer = RedisWriter()
    
    weather_df = reader.read_weather_data()
    weather_clean = cleaner.clean_weather_data(weather_df)
    weather_final = normalizer.normalize_weather_data(weather_clean)
    
    # Ghi MinIO (Data Lake)
    query_minio = minio_writer.write_stream(weather_final, folder="enriched")
    
    # Ghi Redis (Real-time)
    query_redis = weather_final.writeStream \
        .outputMode("append") \
        .foreachBatch(redis_writer.write_stream_to_redis) \
        .option("checkpointLocation", "/tmp/checkpoints/weather_redis") \
        .trigger(processingTime="5 seconds") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_etl_pipeline()