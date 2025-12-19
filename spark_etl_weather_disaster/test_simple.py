"""
Simple test to check if PySpark works with current Python
"""
import os
import sys

# Configure Python for Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from datetime import datetime

print("=" * 80)
print("🧪 TESTING PYSPARK BASIC FUNCTIONALITY")
print("=" * 80)

try:
    # Create Spark session with minimal config
    spark = SparkSession.builder \
        .appName("SimpleTest") \
        .master("local[1]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n✅ Spark session created successfully")
    print(f"   Python version: {sys.version}")
    print(f"   PySpark version: {spark.version}")
    
    # Test 1: Create simple DataFrame
    print("\n📊 Test 1: Creating simple DataFrame...")
    data = [
        {"name": "Alice", "age": 25, "city": "NYC"},
        {"name": "Bob", "age": 30, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "SF"}
    ]
    df = spark.createDataFrame(data)
    print(f"   ✅ DataFrame created with {df.count()} rows")
    df.show()
    
    # Test 2: Simple transformations
    print("\n🔄 Test 2: Testing transformations...")
    from pyspark.sql.functions import col, lit
    df2 = df.withColumn("year", lit(2024))
    print(f"   ✅ Transformation successful")
    df2.show()
    
    # Test 3: Aggregations
    print("\n📈 Test 3: Testing aggregations...")
    avg_age = df.agg({"age": "avg"}).collect()[0][0]
    print(f"   ✅ Average age: {avg_age}")
    
    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED! PySpark is working correctly!")
    print("=" * 80)
    
    spark.stop()

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
