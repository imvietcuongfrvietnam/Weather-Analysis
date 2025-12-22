"""
Weather Forecasting - Main Pipeline
Dự đoán thời tiết sử dụng Spark ML và dữ liệu từ MinIO
"""

from pyspark.sql import SparkSession
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
    print("💡 Đảm bảo bạn đang chạy file này từ thư mục spark/job/ hoặc đã setup PYTHONPATH đúng.")
    sys.exit(1)

# Cấu hình đường dẫn lưu Model/Output cục bộ
LOCAL_MODEL_DIR = "./models_output"
LOCAL_OUTPUT_DIR = "./predictions_output"
TRAIN_TEST_SPLIT = 0.8

def create_spark_session():
    """
    Khởi tạo Spark Session với cấu hình MinIO S3A + PostgreSQL Driver
    """
    print("\n" + "="*80)
    print("🚀 WEATHER FORECASTING ML SYSTEM")
    print("="*80)
    print("⚡ Initializing Spark Session...")
    
    # Sử dụng các version đã kiểm chứng hoạt động ổn định
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

    # =========================================================================
    # 🛠 FIX CỨNG: CẤU HÌNH MINIO TRỰC TIẾP ĐỂ TRÁNH LỖI DNS (UnknownHostException)
    # =========================================================================
    print("🔒 Applying HARDCODED MinIO Configuration...")
    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", "http://weather-minio.default.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # =========================================================================
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark Session initialized successfully!")
    return spark


def run_forecasting_pipeline(city: str = None, limit_rows: int = None, save_models: bool = True):
    
    spark = create_spark_session()
    
    try:
        # ==========================================
        # STEP 1: LOAD DATA FROM MINIO
        # ==========================================
        print("\n" + "="*80)
        print("STEP 1: LOADING DATA FROM MINIO")
        print("="*80)
        
        # Data Loader sẽ sử dụng Spark Session đã có config MinIO chuẩn
        loader = WeatherDataLoader(spark)
        
        # Lưu ý: Đảm bảo DataReader của bạn đọc đúng đường dẫn s3a://weather-data/enriched/weather/
        df = loader.load_data(city=city, limit_rows=limit_rows)
        
        # Kiểm tra xem có dữ liệu không trước khi đi tiếp
        if df is None or df.rdd.isEmpty():
            print("❌ ERROR: Dataframe is empty! Please check if Streaming Job has written data to MinIO.")
            return

        # Validate data
        validation = loader.validate_data(df)
        if validation['quality_score'] < 50:
            print("⚠️ Data quality too poor. Exiting.")
            return

        loader.summary_stats(df)
        
        # ==========================================
        # STEP 2: FEATURE ENGINEERING
        # ==========================================
        print("\n" + "="*80)
        print("STEP 2: FEATURE ENGINEERING")
        print("="*80)
        
        engineer = TimeSeriesFeatureEngineer()
        df_features = engineer.engineer_all_features(df)
        
        feature_cols = engineer.get_feature_columns(df_features, exclude_targets=True)
        print(f"\n📊 Total features created: {len(feature_cols)}")
        
        # ==========================================
        # STEP 3: TRAIN/TEST SPLIT
        # ==========================================
        print("\n" + "="*80)
        print("STEP 3: SPLITTING DATA")
        print("="*80)
        
        # Xóa dòng null (do lag feature tạo ra)
        df_clean = df_features.dropna()
        
        # Split 80/20
        train_df, test_df = df_clean.randomSplit([TRAIN_TEST_SPLIT, 1 - TRAIN_TEST_SPLIT], seed=42)
        
        print(f"Training set:   {train_df.count()} rows")
        print(f"Test set:       {test_df.count()} rows")
        
        if train_df.count() < 50:
            print("❌ Not enough data to train. Need at least 50 rows.")
            return

        # ==========================================
        # STEP 4 & 5: BUILD & TRAIN MODELS
        # ==========================================
        print("\n" + "="*80)
        print("STEP 4 & 5: BUILDING & TRAINING MODELS")
        print("="*80)
        
        model_builder = WeatherForecastModels()
        
        # 1. Build Pipelines
        model_builder.build_all_models(feature_cols)
        
        # 2. Train
        trained_models = model_builder.train_all_models(train_df)
        
        if save_models:
            print(f"\n💾 Saving models to {LOCAL_MODEL_DIR}...")
            if not os.path.exists(LOCAL_MODEL_DIR):
                os.makedirs(LOCAL_MODEL_DIR)
            model_builder.save_all_models(trained_models, LOCAL_MODEL_DIR)
        
        # ==========================================
        # STEP 6: EVALUATE & PREDICT
        # ==========================================
        print("\n" + "="*80)
        print("STEP 6: PREDICTION & EVALUATION")
        print("="*80)
        
        predictions_df = test_df
        # Thực hiện dự đoán cho tất cả các target
        for target, model in trained_models.items():
            predictions_df = model.transform(predictions_df)
            
        evaluator = ForecastEvaluator()
        metrics = evaluator.evaluate_all_models(predictions_df)
        
        print("\n📊 Evaluation Summary:")
        for target, m in metrics.items():
            print(f"   - {target}: RMSE={m.get('rmse', 'N/A'):.4f}, R2={m.get('r2', 'N/A'):.4f}")

        # ==========================================
        # STEP 7: WRITE TO POSTGRESQL 
        # ==========================================
        print("\n" + "="*80)
        print("STEP 7: WRITING TO POSTGRESQL")
        print("="*80)

        # 1. Chọn lọc các cột cần thiết để ghi vào DB
        target_cols = list(config.CONTINUOUS_FEATURES) 
        
        if hasattr(config, 'CATEGORICAL_FEATURES'):
            target_cols += config.CATEGORICAL_FEATURES            
        prediction_cols = [f"prediction_{c}" for c in target_cols]
        
        # Tạo danh sách cột cần select
        select_cols = ['datetime', 'city'] 
        select_cols += [c for c in target_cols if c in predictions_df.columns] # Giá trị thực
        select_cols += [c for c in prediction_cols if c in predictions_df.columns] # Giá trị dự đoán
        
        print(f"   Selecting {len(select_cols)} columns for database...")
        export_df = predictions_df.select(select_cols)
        
        # 2. Gọi Postgres Writer
        pg_writer = PostgresWriter()
        success = pg_writer.write_predictions_safe(export_df)
        
        if success:
            print("   ✅ Database update complete.")
        else:
            print("   ⚠️ Database update skipped/failed.")

        # ==========================================
        # STEP 8: EXPORT CSV & VISUALIZATION
        # ==========================================
        if not os.path.exists(LOCAL_OUTPUT_DIR):
            os.makedirs(LOCAL_OUTPUT_DIR)
            
        output_file = os.path.join(LOCAL_OUTPUT_DIR, f"forecast_{datetime.now().strftime('%Y%m%d')}.csv")
        print(f"\n✅ Pipeline Complete.")
        
        print("\n" + "="*80)
        print("STEP 8: VISUALIZATION")
        print("="*80)

        try:
            viz = ForecastVisualizer()
            # Chuyển Spark DataFrame sang Pandas để vẽ (Chỉ làm khi dữ liệu < 100k dòng)
            pandas_df = predictions_df.toPandas()
            
            # Vẽ biểu đồ Feature Importance & Dự báo
            viz.plot_all_features(pandas_df)
            viz.plot_metrics_comparison(metrics)
            print("   ✅ Visualization charts generated.")
        except Exception as v_err:
            print(f"   ⚠️ Visualization failed (Non-critical): {v_err}")

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