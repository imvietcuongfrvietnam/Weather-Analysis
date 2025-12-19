"""
Test Script - Test MinIO Connection and Data Loading
Kiểm tra kết nối MinIO và đọc dữ liệu
"""

from pyspark.sql import SparkSession
import config

def test_spark_minio_connection():
    """Test Spark connection to MinIO"""
    print("\n" + "="*80)
    print("🧪 TESTING MINIO CONNECTION")
    print("="*80)
    
    # Create Spark session
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    builder = SparkSession.builder \
        .appName("TestMinIO") \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars.ivy", "/tmp/.ivy2")
    
    # Add S3A config
    for key, value in config.SPARK_S3A_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    
    try:
        # Try to read from MinIO
        print(f"\n📂 Attempting to read from: {config.MINIO_INPUT_PATH}")
        
        df = spark.read.parquet(config.MINIO_INPUT_PATH)
        
        count = df.count()
        print(f"✅ Successfully read {count} records from MinIO!")
        
        print("\n📊 Schema:")
        df.printSchema()
        
        print("\n📋 Sample data:")
        df.show(5, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error reading from MinIO: {e}")
        print("\nPossible causes:")
        print("  1. MinIO is not running")
        print("  2. No data in MinIO (run ETL pipeline first)")
        print("  3. Incorrect MinIO credentials")
        print("  4. Network connection issues")
        return False
        
    finally:
        spark.stop()


def test_modules_import():
    """Test that all modules can be imported"""
    print("\n" + "="*80)
    print("🧪 TESTING MODULE IMPORTS")
    print("="*80)
    
    try:
        print("Importing data_loader...", end=" ")
        from data_loader import WeatherDataLoader
        print("✅")
        
        print("Importing feature_engineering...", end=" ")
        from feature_engineering import TimeSeriesFeatureEngineer
        print("✅")
        
        print("Importing models...", end=" ")
        from models import WeatherForecastModels
        print("✅")
        
        print("Importing forecast_evaluator...", end=" ")
        from forecast_evaluator import ForecastEvaluator
        print("✅")
        
        print("Importing visualization...", end=" ")
        from visualization import ForecastVisualizer
        print("✅")
        
        print("\n✅ All modules imported successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Import error: {e}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("🚀 WEATHER FORECASTING ML - SYSTEM TESTS")
    print("="*80)
    
    # Test 1: Module imports
    test1 = test_modules_import()
    
    # Test 2: MinIO connection
    test2 = test_spark_minio_connection()
    
    # Summary
    print("\n" + "="*80)
    print("📝 TEST SUMMARY")
    print("="*80)
    print(f"Module Imports:      {'✅ PASS' if test1 else '❌ FAIL'}")
    print(f"MinIO Connection:    {'✅ PASS' if test2 else '❌ FAIL'}")
    print("="*80 + "\n")
    
    if test1 and test2:
        print("✅ ALL TESTS PASSED! System is ready to use.")
        print("\nNext steps:")
        print("  python weather_forecasting.py")
    else:
        print("⚠️  SOME TESTS FAILED. Please check the errors above.")
        if not test2:
            print("\n💡 If MinIO test failed, make sure:")
            print("   1. MinIO is running (docker ps)")
            print("   2. ETL pipeline has written data to MinIO")
            print("   3. Check spark_etl_weather_disaster/main_etl.py")


if __name__ == "__main__":
    main()
