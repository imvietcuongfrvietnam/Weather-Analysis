"""
Weather Forecasting - Main Pipeline (Final Version)
- Train & Evaluate Model
- Ghi kết quả vào PostgreSQL (Mode Overwrite)
- Vẽ biểu đồ (Fix lỗi datetime64)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_add, expr, current_timestamp
import sys
import os
import argparse
from datetime import datetime

# --- IMPORT MODULES ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import config
    from visualization import ForecastVisualizer
    from data_loader import WeatherDataLoader
    from feature_engineering import TimeSeriesFeatureEngineer
    from models import WeatherForecastModels
    from forecast_evaluator import ForecastEvaluator
    from postgres_writer import PostgresWriter
except ImportError as e:
    print(f"❌ Lỗi Import: {e}")
    sys.exit(1)

# Cấu hình đường dẫn lưu Model/Output cục bộ
LOCAL_MODEL_DIR = "./models_output"
LOCAL_OUTPUT_DIR = "./predictions_output"
TRAIN_TEST_SPLIT = 0.8

def create_spark_session():
    print("\n" + "="*80)
    print("🚀 WEATHER FORECASTING ML SYSTEM")
    print("="*80)
    print("⚡ Initializing Spark Session...")
    
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        "org.postgresql:postgresql:42.5.0"
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherForecast_Training") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars.ivy", "/tmp/.ivy2")

    # FIX CỨNG MINIO
    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", "http://weather-minio.default.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_forecasting_pipeline(city: str = None, limit_rows: int = None, save_models: bool = True):
    
    spark = create_spark_session()
    
    try:
        # ==========================================
        # STEP 1: LOAD DATA
        # ==========================================
        print("\nSTEP 1: LOADING DATA FROM MINIO")
        loader = WeatherDataLoader(spark)
        df = loader.load_data(city=city, limit_rows=limit_rows)
        
        if df is None or df.rdd.isEmpty():
            print("❌ Dataframe is empty!")
            return

        # ==========================================
        # STEP 2: FEATURE ENGINEERING
        # ==========================================
        print("\nSTEP 2: FEATURE ENGINEERING")
        engineer = TimeSeriesFeatureEngineer()
        df_features = engineer.engineer_all_features(df)
        feature_cols = engineer.get_feature_columns(df_features, exclude_targets=True)
        
        # ==========================================
        # STEP 3: SPLIT DATA
        # ==========================================
        print("\nSTEP 3: SPLITTING DATA")
        df_clean = df_features.dropna()
        train_df, test_df = df_clean.randomSplit([TRAIN_TEST_SPLIT, 1 - TRAIN_TEST_SPLIT], seed=42)
        print(f"Training set: {train_df.count()} | Test set: {test_df.count()}")
        
        if train_df.count() < 50:
            print("❌ Not enough data to train.")
            return

        # ==========================================
        # STEP 4 & 5: TRAIN MODELS
        # ==========================================
        print("\nSTEP 4 & 5: TRAINING MODELS")
        model_builder = WeatherForecastModels()
        model_builder.build_all_models(feature_cols)
        trained_models = model_builder.train_all_models(train_df)
        
        if save_models:
            print(f"💾 Saving models to {LOCAL_MODEL_DIR}...")
            model_builder.save_all_models(trained_models, LOCAL_MODEL_DIR)
        
        # ==========================================
        # STEP 6: EVALUATE (Test Set)
        # ==========================================
        print("\nSTEP 6: EVALUATION (HISTORICAL)")
        predictions_test_df = test_df
        for target, model in trained_models.items():
            predictions_test_df = model.transform(predictions_test_df)
            
        evaluator = ForecastEvaluator()
        metrics = evaluator.evaluate_all_models(predictions_test_df)
        
        print("\n📊 Evaluation Summary:")
        for target, m in metrics.items():
            print(f"   - {target}: RMSE={m.get('rmse', 'N/A'):.4f}, R2={m.get('r2', 'N/A'):.4f}")

        # ==========================================
        # STEP 7: WRITE TO POSTGRESQL (ENABLED)
        # ==========================================
        print("\n" + "="*80)
        print("STEP 7: WRITING TO POSTGRESQL")
        print("="*80)

        # 1. Chọn các cột cần thiết để ghi
        # Danh sách cột mục tiêu (cần đảm bảo khớp với config)
        target_features = ['temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction']
        
        cols_to_select = ['datetime', 'city']
        
        # Thêm cột thực tế (Actual)
        for f in target_features:
            if f in predictions_test_df.columns:
                cols_to_select.append(f)
        
        # Thêm cột dự báo (Prediction)
        for f in target_features:
            pred_col = f"prediction_{f}"
            if pred_col in predictions_test_df.columns:
                cols_to_select.append(pred_col)
                
        # Thêm weather_desc nếu có
        if 'weather_desc' in predictions_test_df.columns:
            cols_to_select.append('weather_desc')
        if 'prediction_weather_desc' in predictions_test_df.columns:
            cols_to_select.append('prediction_weather_desc')

        # Tạo DataFrame cuối cùng để ghi
        final_db_df = predictions_test_df.select(*cols_to_select) \
                                         .withColumn("created_at", current_timestamp())

        print(f"   Writing {final_db_df.count()} records to database...")
        
        # 2. Gọi Writer (sẽ dùng mode 'overwrite' trong postgres_writer.py)
        pg_writer = PostgresWriter()
        success = pg_writer.write_predictions_safe(final_db_df)
        
        if success:
            print("   ✅ Database update complete.")
        else:
            print("   ⚠️ Database update skipped/failed.")

        # ==========================================
        # STEP 8: VISUALIZATION (Fix Datetime)
        # ==========================================
        print("\n" + "="*80)
        print("STEP 8: VISUALIZATION")
        print("="*80)

        if not os.path.exists(LOCAL_OUTPUT_DIR):
            os.makedirs(LOCAL_OUTPUT_DIR)

        try:
            viz = ForecastVisualizer()
            
            # 1. Convert to Pandas
            pandas_df = predictions_test_df.toPandas()
            
            # 2. FIX LỖI DATETIME64
            import pandas as pd
            if 'datetime' in pandas_df.columns:
                pandas_df['datetime'] = pandas_df['datetime'].astype('datetime64[ns]')
            
            # 3. Vẽ biểu đồ
            viz.plot_all_features(pandas_df)
            viz.plot_metrics_comparison(metrics)
            print("   ✅ Visualization charts generated successfully!")
            
        except Exception as v_err:
            print(f"   ⚠️ Visualization failed: {v_err}")

        print(f"\n✅ Pipeline Complete.")

    except Exception as e:
        print(f"\n❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--city', type=str, default=None)
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--no-save', action='store_true')
    args = parser.parse_args()
    
    run_forecasting_pipeline(
        city=args.city, 
        limit_rows=args.limit,
        save_models=not args.no_save
    )