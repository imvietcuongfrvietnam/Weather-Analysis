"""
MAIN ETL PIPELINE
Spark ETL Batch cho Weather & Disaster Prediction

CHẠY: python main_etl.py
"""

from pyspark.sql import SparkSession
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from readers.data_readers import FakeDataReader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from transformations.enrichment import DataEnricher
from writers.data_writers import FakeDataWriter


def create_spark_session():
    """Initialize Spark session"""
    spark = SparkSession.builder \
        .appName("WeatherDisasterETL") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_etl_pipeline():
    """
    Main ETL Pipeline
    
    Flow:
    1. READ: Đọc data từ 4 nguồn (fake)
    2. CLEAN: Clean data
    3. NORMALIZE: Normalize units/formats
    4. ENRICH: Add computed fields, risk scores
    5. WRITE: Save to HDFS + Elasticsearch (fake)
    """
    
    print("\n" + "="*80)
    print("🚀 SPARK ETL - WEATHER & DISASTER PREDICTION - NYC")
    print("="*80)
    
    # ===========================
    # 1. INITIALIZE SPARK
    # ===========================
    spark = create_spark_session()
    reader = FakeDataReader(spark)
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    enricher = DataEnricher()
    writer = FakeDataWriter()
    
    print("\n✅ Spark session initialized")
    
    # ===========================
    # 2. READ DATA (FAKE)
    # ===========================
    print("\n" + "="*80)
    print("📖 STEP 1: READING DATA FROM 4 SOURCES")
    print("="*80)
    
    # Read weather data
    weather_df = reader.read_weather_data(num_records=1000)
    
    # Read 311 requests
    service_311_df = reader.read_311_requests(num_records=500)
    
    # Read taxi trips
    taxi_df = reader.read_taxi_trips(num_records=800)
    
    # Read collisions
    collision_df = reader.read_collisions(num_records=300)
    
    print("\n✅ Data reading complete!")
    print(f"   - Weather: {weather_df.count()} records")
    print(f"   - 311 Requests: {service_311_df.count()} records")
    print(f"   - Taxi Trips: {taxi_df.count()} records")
    print(f"   - Collisions: {collision_df.count()} records")
    
    # ===========================
    # 3. CLEAN DATA
    # ===========================
    print("\n" + "="*80)
    print("🧹 STEP 2: CLEANING DATA")
    print("="*80)
    
    weather_clean = cleaner.clean_weather_data(weather_df)
    service_311_clean = cleaner.clean_311_data(service_311_df)
    taxi_clean = cleaner.clean_taxi_data(taxi_df)
    collision_clean = cleaner.clean_collision_data(collision_df)
    
    print("\n✅ Data cleaning complete!")
    
    # ===========================
    # 4. NORMALIZE DATA
    # ===========================
    print("\n" + "="*80)
    print("📏 STEP 3: NORMALIZING DATA")
    print("="*80)
    
    weather_norm = normalizer.normalize_weather_data(weather_clean)
    service_311_norm = normalizer.normalize_311_data(service_311_clean)
    taxi_norm = normalizer.normalize_taxi_data(taxi_clean)
    collision_norm = normalizer.normalize_collision_data(collision_clean)
    
    print("\n✅ Data normalization complete!")
    
    # ===========================
    # 5. ENRICH DATA
    # ===========================
    print("\n" + "="*80)
    print("✨ STEP 4: ENRICHING DATA")
    print("="*80)
    
    # Add disaster risk scores to weather
    weather_enriched = enricher.enrich_with_disaster_risk(weather_norm)
    
    # Add traffic impact (join weather + taxi + collision)
    integrated_df = enricher.enrich_with_traffic_impact(
        spark,
        weather_enriched,
        taxi_norm,
        collision_norm
    )
    
    # Add ML features
    final_df = enricher.add_ml_features(integrated_df)
    
    # Add processing metadata
    final_df = enricher.add_processing_metadata(final_df)
    
    print("\n✅ Data enrichment complete!")
    
    # ===========================
    # 6. WRITE DATA (FAKE)
    # ===========================
    print("\n" + "="*80)
    print("💾 STEP 5: WRITING DATA")
    print("="*80)
    
    # Write to console (preview)
    writer.write_to_console(final_df, name="Integrated Weather-Disaster Data", num_rows=10)
    
    # Write to fake HDFS
    writer.write_to_fake_hdfs(
        final_df,
        path="weather_disaster_integrated",
        format="parquet",
        mode="overwrite"
    )
    
    # Write to fake Elasticsearch
    writer.write_to_fake_elasticsearch(
        final_df,
        index="weather-disaster-nyc"
    )
    
    # ===========================
    # 7. STATISTICS & SUMMARY
    # ===========================
    print("\n" + "="*80)
    print("📊 STEP 6: SUMMARY STATISTICS")
    print("="*80)
    
    print("\n🌦️  Weather Statistics:")
    final_df.groupBy("weather_condition").count().show()
    
    print("\n⚠️  Emergency Level Distribution:")
    final_df.groupBy("emergency_level").count().show()
    
    print("\n🚗 Traffic Impact by Weather:")
    final_df.groupBy("weather_condition") \
        .agg({
            "trip_count": "avg",
            "collision_count": "avg",
            "traffic_impact_score": "avg"
        }).show()
    
    print("\n📈 Disaster Risk Score Stats:")
    final_df.select("disaster_risk_score").describe().show()
    
    # ===========================
    # 8. COMPLETION
    # ===========================
    print("\n" + "="*80)
    print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*80)
    
    print("\n📋 Pipeline Summary:")
    print(f"   ✓ Total records processed: {final_df.count()}")
    print(f"   ✓ Total columns: {len(final_df.columns)}")
    print(f"   ✓ Data sources integrated: 4 (Weather, 311, Taxi, Collisions)")
    print(f"   ✓ Outputs generated: Console + Fake HDFS + Fake Elasticsearch")
    
    print("\n🎯 Next Steps:")
    print("   1. Replace FakeDataReader with real Kafka/HDFS readers")
    print("   2. Replace FakeDataWriter with real HDFS/Elasticsearch writers")
    print("   3. Add error handling and logging")
    print("   4. Deploy to Spark cluster")
    print("   5. Schedule with Airflow")
    
    print("\n" + "="*80)
    
    # Stop Spark
    spark.stop()


if __name__ == "__main__":
    try:
        run_etl_pipeline()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
