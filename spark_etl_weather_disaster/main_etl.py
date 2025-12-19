"""
MAIN ETL PIPELINE
Spark ETL Batch cho Weather & Disaster Prediction

CHẠY: python main_etl.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os

# Configure Python executable for Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from readers.real_data_reader import DataReader  # NEW: Use real reader
from transformations.cleaning import DataCleaner
from transformations.normalization import DataNormalizer
from transformations.enrichment import DataEnricher
from writers.data_writers import FakeDataWriter  # Keep for legacy output
from writers.real_data_writer import DataWriter  # NEW: Use real writer
from streaming_helpers import (  # NEW: Streaming mode helpers
    is_streaming,
    show_data_smart,
    count_data_smart,
    stop_all_streaming_queries
)


def create_spark_session():
    """Initialize Spark session with MinIO/S3 support"""
    # Import MinIO S3 configuration
    try:
        from minio_config import SPARK_S3_CONFIG
        has_minio_config = True
    except ImportError:
        print("⚠️  minio_config.py not found. Running without MinIO support.")
        has_minio_config = False
    
    # Build Spark session
    builder = SparkSession.builder \
        .appName("WeatherDisasterETL") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1")
    
    # Add MinIO/S3 configurations if available
    if has_minio_config:
        print("📦 Adding MinIO/S3 configuration to Spark...")
        for key, value in SPARK_S3_CONFIG.items():
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
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
    
    # Choose data source: "json" (from files) or "kafka" (from Kafka)
    # Choose Kafka mode: "batch" (micro-batches, easier) or "streaming" (real-time)
    # 
    # BATCH MODE:    Kafka → read() → DataFrame → count/show/write (compatible)
    # STREAMING MODE: Kafka → readStream() → DataFrame → writeStream (advanced)
    #
    reader = DataReader(spark, source_type="json", kafka_mode="batch")  
    # To use Kafka: source_type="kafka", kafka_mode="batch" hoặc "streaming"
    
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    enricher = DataEnricher()
    writer = FakeDataWriter()
    
    print("\n✅ Spark session initialized")
    print(f"📂 Data source: {reader.source_type}")
    if reader.source_type == "kafka":
        print(f"⚡ Kafka mode: {reader.kafka_mode.upper()}")
    
    # Detect if we're in streaming mode
    is_streaming_mode = (reader.source_type == "kafka" and reader.kafka_mode == "streaming")
    
    # ===========================
    # 2. READ DATA FROM FILES
    # ===========================
    print("\n" + "="*80)
    print("📖 STEP 1: READING DATA FROM PRE-GENERATED FILES")
    print("="*80)
    print("💡 Data is read from ./data/*.json (generated once by generate_data.py)")
    
    # Read weather data
    weather_df = reader.read_weather_data()
    
    # Read 311 requests
    service_311_df = reader.read_311_requests()
    
    # Read taxi trips
    taxi_df = reader.read_taxi_trips()
    
    # Read collisions
    collision_df = reader.read_collisions()
    
    print("\n✅ Data reading complete!")
    print(f"   - Weather: {count_data_smart(weather_df)} records")
    print(f"   - 311 Requests: {count_data_smart(service_311_df)} records")
    print(f"   - Taxi Trips: {count_data_smart(taxi_df)} records")
    print(f"   - Collisions: {count_data_smart(collision_df)} records")
    
    # ===========================
    # 3. CLEAN DATA
    # ===========================
    print("\n" + "="*80)
    print("🧹 STEP 2: CLEANING DATA")
    print("="*80)
    
    # Show BEFORE cleaning (streaming-aware)
    print("\n📊 WEATHER DATA - BEFORE CLEANING:")
    if not is_streaming_mode:
        print(f"   Total records: {weather_df.count()}")
        print(f"   Records with nulls: {weather_df.filter(col('temperature').isNull() | col('city').isNull()).count()}")
        weather_df.select("datetime", "city", "temperature", "humidity", "pressure").show(5, truncate=False)
    else:
        print(f"   ⚡ Streaming mode - skipping count and show")
    
    weather_clean = cleaner.clean_weather_data(weather_df)
    service_311_clean = cleaner.clean_311_data(service_311_df)
    taxi_clean = cleaner.clean_taxi_data(taxi_df)
    collision_clean = cleaner.clean_collision_data(collision_df)
    
    # Show AFTER cleaning (streaming-aware)
    print("\n📊 WEATHER DATA - AFTER CLEANING:")
    if not is_streaming_mode:
        print(f"   Total records: {weather_clean.count()}")
        print(f"   Records with nulls: {weather_clean.filter(col('temperature').isNull() | col('city').isNull()).count()}")
        weather_clean.select("datetime", "city", "temperature", "humidity", "pressure").show(5, truncate=False)
    else:
        print(f"   ⚡ Streaming mode - skipping count and show")
    
    print("\n✅ Data cleaning complete!")
    
    # ===========================
    # 3.5 SAVE CLEANED DATA
    # ===========================
    print("\n" + "="*80)
    print("💾 SAVING CLEANED DATA TO FILES")
    print("="*80)
    print("💡 Cleaned data is saved to ./output/*.json for inspection")
    
    # Initialize DataWriter
    # Use "minio" for MinIO storage, or "json" for local testing
    # Change to output_type="minio" when MinIO server is ready
    data_writer = DataWriter(output_type="json")  # TODO: Change to "minio" when ready
    
    # Save cleaned data for each dataset
    data_writer.write_cleaned_data(weather_clean, "weather")
    data_writer.write_cleaned_data(service_311_clean, "311_requests")
    data_writer.write_cleaned_data(taxi_clean, "taxi_trips")
    data_writer.write_cleaned_data(collision_clean, "collisions")
    
    print("\n✅ Cleaned data saved to ./output/ directory!")
    print("   📂 You can now inspect the cleaned JSON files")
    
    # ===========================
    # 4. NORMALIZE DATA
    # ===========================
    print("\n" + "="*80)
    print("📏 STEP 3: NORMALIZING DATA")
    print("="*80)
    
    # Show BEFORE normalization (streaming-aware)
    print("\n📊 WEATHER DATA - BEFORE NORMALIZATION:")
    if not is_streaming_mode:
        weather_clean.select("datetime", "city", "temperature", "wind_speed").show(3, truncate=False)
    else:
        print(f"   ⚡ Streaming mode - skipping show")
    
    weather_norm = normalizer.normalize_weather_data(weather_clean)
    service_311_norm = normalizer.normalize_311_data(service_311_clean)
    taxi_norm = normalizer.normalize_taxi_data(taxi_clean)
    collision_norm = normalizer.normalize_collision_data(collision_clean)
    
    # Show AFTER normalization (streaming-aware)
    print("\n📊 WEATHER DATA - AFTER NORMALIZATION:")
    if not is_streaming_mode:
        weather_norm.select("datetime", "city", "temp_celsius", "temp_fahrenheit", "wind_speed_kmh").show(3, truncate=False)
    else:
        print(f"   ⚡ Streaming mode - skipping show")
    
    print("\n✅ Data normalization complete!")
    
    # ===========================
    # 5. ENRICH DATA
    # ===========================
    print("\n" + "="*80)
    print("✨ STEP 4: ENRICHING DATA")
    print("="*80)
    
    # Show BEFORE enrichment (streaming-aware)
    print("\n📊 WEATHER DATA - BEFORE ENRICHMENT:")
    print(f"   Number of columns: {len(weather_norm.columns)}")
    if not is_streaming_mode:
        print(f"   Columns: {', '.join(weather_norm.columns[:10])}...")
    else:
        print(f"   ⚡ Streaming mode")
    
    # Add disaster risk scores to weather
    weather_enriched = enricher.enrich_with_disaster_risk(weather_norm)
    
    # Show AFTER adding disaster risk (streaming-aware)
    print("\n📊 WEATHER DATA - AFTER DISASTER RISK CALCULATION:")
    if not is_streaming_mode:
        weather_enriched.select("datetime", "city", "weather_condition", "disaster_risk_score", "emergency_level").show(5, truncate=False)
    else:
        print(f"   ⚡ Streaming mode - skipping show")
    
    # Add traffic impact (join weather + taxi + collision)
    integrated_df = enricher.enrich_with_traffic_impact(
        spark,
        weather_enriched,
        taxi_norm,
        collision_norm
    )
    
    print("\n📊 INTEGRATED DATA - AFTER TRAFFIC IMPACT:")
    if not is_streaming_mode:
        integrated_df.select("datetime", "weather_condition", "trip_count", "collision_count", "traffic_impact_score").show(5, truncate=False)
    else:
        print(f"   ⚡ Streaming mode - skipping show")
    
    # Add ML features
    final_df = enricher.add_ml_features(integrated_df)
    
    # Add processing metadata
    final_df = enricher.add_processing_metadata(final_df)
    
    # Show FINAL result
    print("\n📊 FINAL ENRICHED DATA:")
    print(f"   Total columns: {len(final_df.columns)}")
    print(f"   New features added: disaster_risk_score, traffic_impact_score, is_weekend, season, weather_comfort_index, etc.")
    
    print("\n✅ Data enrichment complete!")
    
    # ===========================
    # 6. SAVE FINAL ENRICHED DATA
    # ===========================
    print("\n" + "="*80)
    print("💾 STEP 5: SAVING FINAL ENRICHED DATA")
    print("="*80)
    
    # Save final enriched data to JSON (can switch to HDFS/Elasticsearch later)
    data_writer.write_enriched_data(final_df, dataset_name="integrated")
    
    print("\n✅ Final enriched data saved!")
    
    # ===========================
    # 7. PREVIEW & OUTPUTS
    # ===========================
    print("\n" + "="*80)
    print("💾 STEP 6: PREVIEW & FINAL OUTPUTS")
    print("="*80)
    
    # Write to console (preview)
    writer.write_to_console(final_df, name="Integrated Weather-Disaster Data", num_rows=10)
    
    # Save intermediate stages to local files for inspection
    print("\n💾 Saving intermediate stages locally for inspection...")
    writer.write_to_fake_hdfs(weather_clean, path="stage_1_cleaned_weather", format="csv", mode="overwrite")
    writer.write_to_fake_hdfs(weather_norm, path="stage_2_normalized_weather", format="csv", mode="overwrite")
    writer.write_to_fake_hdfs(weather_enriched, path="stage_3_enriched_weather", format="csv", mode="overwrite")
    print("   ✅ Saved to: ./fake_output/stage_1_cleaned_weather/")
    print("   ✅ Saved to: ./fake_output/stage_2_normalized_weather/")
    print("   ✅ Saved to: ./fake_output/stage_3_enriched_weather/")
    
    # ===========================
    # 8. STATISTICS & SUMMARY (streaming-aware)
    # ===========================
    # ===========================
    print("\n" + "="*80)
    print("📊 STEP 7: SUMMARY STATISTICS")
    print("="*80)
    
    if not is_streaming_mode:
        # BATCH MODE: Show statistics
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
    else:
        # STREAMING MODE: Statistics will be computed in streaming query output
        print("\n⚡ STREAMING MODE:")
        print("   Statistics will be available in streaming query outputs")
        print("   Use Spark UI or streaming queries to monitor data")
    
    # ===========================
    print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*80)
    
    print("\n📋 Pipeline Summary:")
    if not is_streaming_mode:
        print(f"   ✓ Total records processed: {final_df.count()}")
    else:
        print(f"   ✓ Mode: STREAMING (real-time processing)")
    print(f"   ✓ Total columns: {len(final_df.columns)}")
    print(f"   ✓ Data sources integrated: 4 (Weather, 311, Taxi, Collisions)")
    print(f"   ✓ Outputs: Local JSON/CSV + MinIO (when configured)")
    
    # Special handling for streaming mode
    if is_streaming_mode:
        print("\n⚡ STREAMING MODE ACTIVE")
        print("   The pipeline is now continuously processing data from Kafka")
        print("   Press Ctrl+C to stop the streaming queries")
        print("\n   Active Queries:")
        for query in spark.streams.active:
            print(f"      - {query.name}: {query.status}")
        
        print("\n   Waiting for streaming queries... (Press Ctrl+C to stop)")
        try:
            # Keep pipeline running for streaming
            spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping streaming queries...")
            stop_all_streaming_queries(spark)
            print("✅ All streaming queries stopped successfully")
    
    print("\n🎯 Next Steps:")
    if reader.source_type == "json":
        print("   1. Setup MinIO server (Docker or standalone)")
        print("   2. Update minio_config.py with real credentials")
        print("   3. Change DataWriter to output_type='minio'")
        print("   4. Setup Kafka server")
        print("   5. Update kafka_config.py with Kafka details")
        print("   6. Change to source_type='kafka', kafka_mode='batch' or 'streaming'")
    elif reader.kafka_mode == "batch":
        print("   1. Consider upgrading to streaming mode for real-time processing")
        print("   2. Add error handling and monitoring")
        print("   3. Deploy to Spark cluster")
    else: # This covers streaming mode
        print("   1. Monitor streaming queries via Spark UI")
        print("   2. Set up alerting for emergency levels")
        print("   3. Configure auto-scaling for high traffic")
    
    print("\n" + "="*80)
    
    # Stop Spark (only in batch mode)
    if not is_streaming_mode:
        spark.stop()


if __name__ == "__main__":
    try:
        run_etl_pipeline()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
