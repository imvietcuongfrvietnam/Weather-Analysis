"""
Data Generator - Generate fake data ONCE and save to files
Run this script only when you need to regenerate test data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
import sys
import os
import json

# Configure Python for Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from readers.data_readers import FakeDataReader

def generate_and_save_data():
    """Generate fake data and save to JSON files"""
    
    print("\n" + "="*80)
    print("🔧 DATA GENERATOR - Generate Test Data Once")
    print("="*80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DataGenerator") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    reader = FakeDataReader(spark)
    
    # Create data directory
    data_dir = "./data"
    os.makedirs(data_dir, exist_ok=True)
    
    print("\n📊 Generating fake data...")
    
    # Generate weather data
    print("\n1️⃣ Generating weather data (1000 records)...")
    weather_df = reader.read_weather_data(num_records=1000)
    weather_path = f"{data_dir}/weather_data.csv"
    # Format timestamp columns to string before saving to CSV
    timestamp_cols = [field.name for field in weather_df.schema.fields if str(field.dataType) == 'TimestampType()']
    for ts_col in timestamp_cols:
        weather_df = weather_df.withColumn(ts_col, date_format(col(ts_col), "yyyy-MM-dd HH:mm:ss"))
    # Use pandas to avoid Hadoop winutils issues on Windows
    weather_df.toPandas().to_csv(weather_path, index=False)
    print(f"   ✅ Saved to: {weather_path}")
    
    # Generate 311 requests
    print("\n2️⃣ Generating 311 requests (500 records)...")
    service_311_df = reader.read_311_requests(num_records=500)
    service_311_path = f"{data_dir}/311_requests.csv"
    # Format timestamp columns to string before saving to CSV
    timestamp_cols = [field.name for field in service_311_df.schema.fields if str(field.dataType) == 'TimestampType()']
    for ts_col in timestamp_cols:
        service_311_df = service_311_df.withColumn(ts_col, date_format(col(ts_col), "yyyy-MM-dd HH:mm:ss"))
    service_311_df.toPandas().to_csv(service_311_path, index=False)
    print(f"   ✅ Saved to: {service_311_path}")
    
    # Generate taxi trips
    print("\n3️⃣ Generating taxi trips (800 records)...")
    taxi_df = reader.read_taxi_trips(num_records=800)
    taxi_path = f"{data_dir}/taxi_trips.csv"
    # Format timestamp columns to string before saving to CSV
    timestamp_cols = [field.name for field in taxi_df.schema.fields if str(field.dataType) == 'TimestampType()']
    for ts_col in timestamp_cols:
        taxi_df = taxi_df.withColumn(ts_col, date_format(col(ts_col), "yyyy-MM-dd HH:mm:ss"))
    taxi_df.toPandas().to_csv(taxi_path, index=False)
    print(f"   ✅ Saved to: {taxi_path}")
    
    # Generate collisions
    print("\n4️⃣ Generating collision data (300 records)...")
    collision_df = reader.read_collisions(num_records=300)
    collision_path = f"{data_dir}/collisions.csv"
    # For collision data: merge crash_date + crash_time into single datetime column BEFORE saving
    # This avoids inferSchema issues when reading CSV later
    from pyspark.sql.functions import concat, lit, to_timestamp
    if "crash_date" in collision_df.columns and "crash_time" in collision_df.columns:
        collision_df = collision_df.withColumn(
            "crash_datetime",
            to_timestamp(concat(col("crash_date").cast("string"), lit(" "), col("crash_time").cast("string")))
        )
        # Format crash_datetime to string
        collision_df = collision_df.withColumn("crash_datetime", date_format(col("crash_datetime"), "yyyy-MM-dd HH:mm:ss"))
    # Format any other timestamp columns
    timestamp_cols = [field.name for field in collision_df.schema.fields if str(field.dataType) == 'TimestampType()']
    for ts_col in timestamp_cols:
        if ts_col != "crash_datetime":  # Skip if already formatted
            collision_df = collision_df.withColumn(ts_col, date_format(col(ts_col), "yyyy-MM-dd HH:mm:ss"))
    collision_df.toPandas().to_csv(collision_path, index=False)
    print(f"   ✅ Saved to: {collision_path}")
    
    # Create metadata file
    metadata = {
        "generated_at": "2025-12-02",
        "data_sources": {
            "weather": {
                "path": weather_path,
                "records": weather_df.count(),
                "description": "NYC weather data (fake)"
            },
            "311_requests": {
                "path": service_311_path,
                "records": service_311_df.count(),
                "description": "NYC 311 service requests (fake)"
            },
            "taxi_trips": {
                "path": taxi_path,
                "records": taxi_df.count(),
                "description": "NYC taxi trips (fake)"
            },
            "collisions": {
                "path": collision_path,
                "records": collision_df.count(),
                "description": "NYC collision data (fake)"
            }
        }
    }
    
    metadata_path = f"{data_dir}/metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    print("\n" + "="*80)
    print("✅ DATA GENERATION COMPLETE!")
    print("="*80)
    print(f"\n📁 Data saved to: ./{data_dir}/")
    print(f"   - weather_data.csv ({weather_df.count()} records)")
    print(f"   - 311_requests.csv ({service_311_df.count()} records)")
    print(f"   - taxi_trips.csv ({taxi_df.count()} records)")
    print(f"   - collisions.csv ({collision_df.count()} records)")
    print(f"   - metadata.json")
    print("\n💡 Now you can run main_etl.py to process this data!")
    print("\n📝 Note: CSV format is used for Windows compatibility")
    
    spark.stop()

if __name__ == "__main__":
    generate_and_save_data()
