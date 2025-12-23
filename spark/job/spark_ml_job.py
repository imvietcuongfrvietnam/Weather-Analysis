"""
Weather Forecasting - Main Pipeline
Dự đoán thời tiết sử dụng Spark ML và dữ liệu từ MinIO
Updated: Added Future Forecast Generation (7 Days) & Hardcoded MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_add, expr
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

def generate_future_forecast(spark, model_dict, last_known_data, days=7):
    """
    Sinh ra dự báo cho 7 ngày tiếp theo dựa trên dữ liệu ngày cuối cùng (Persistence Strategy)
    """
    print(f"\n🔮 Generating forecast for next {days} days...")
    
    # 1. Lấy dòng dữ liệu cuối cùng làm cơ sở
    # Sắp xếp giảm dần theo thời gian và lấy 1 dòng đầu tiên
    last_row = last_known_data.orderBy(col("datetime").desc()).limit(1)
    
    if last_row.count() == 0:
        print("⚠️ No data to generate forecast from.")
        return None

    future_preds = []
    
    # 2. Vòng lặp sinh 7 ngày
    # Giả định: Các yếu tố đầu vào (features) cho ngày mai tương tự ngày hôm nay (Persistence)
    # Mô hình sẽ dùng input đó để predict ra output.
    for i in range(1, days + 1):
        # Tạo ngày tương lai: last_date + i
        next_day = last_row.withColumn("datetime", date_add(col("datetime"), i))
        
        row_dict = {}
        # Lấy datetime và city ra để lưu
        row_dict['datetime'] = next_day.select("datetime").collect()[0][0]
        row_dict['city'] = next_day.select("city").collect()[0][0]
        
        # Dự đoán từng chỉ số bằng các model đã train
        for target, model in model_dict.items():
            # Dự đoán
            pred_df = model.transform(next_day)
            # Lấy giá trị prediction
            # Cột output của model thường tên là f"prediction_{target}" hoặc "prediction" tùy config
            # Trong file models.py ta đã set outputCol=f"prediction_{target}"
            pred_col_name = f"prediction_{target}"
            
            if pred_col_name in pred_df.columns:
                val = pred_df.select(pred_col_name).collect()[0][0]
                row_dict[pred_col_name] = val
            
        future_preds.append(row_dict)
        
    # 3. Tạo DataFrame từ list dự báo
    if not future_preds:
        return None
        
    future_df = spark.createDataFrame(future_preds)
    print(f"   ✅ Generated {days} future days.")
    return future_df

def run_forecasting_pipeline(city: str = None, limit_rows: int = None, save_models: bool = True):
    
    spark = create_spark_session()
    
    try:
        # ==========================================
        # STEP 1: LOAD DATA FROM MINIO
        # ==========================================
        print("\n" + "="*80)
        print("STEP 1: LOADING DATA FROM MINIO")
        print("="*80)
        
        loader = WeatherDataLoader(spark)
        df = loader.load_data(city=city, limit_rows=limit_rows)
        
        if df is None or df.rdd.isEmpty():
            print("❌ ERROR: Dataframe is empty! Please check if Streaming Job has written data to MinIO.")
            return

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
        
        df_clean = df_features.dropna()
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
        model_builder.build_all_models(feature_cols)
        trained_models = model_builder.train_all_models(train_df)
        
        if save_models:
            print(f"\n💾 Saving models to {LOCAL_MODEL_DIR}...")
            if not os.path.exists(LOCAL_MODEL_DIR):
                os.makedirs(LOCAL_MODEL_DIR)
            model_builder.save_all_models(trained_models, LOCAL_MODEL_DIR)
        
        # ==========================================
        # STEP 6: EVALUATE (Test Set - Historical)
        # ==========================================
        print("\n" + "="*80)
        print("STEP 6: EVALUATION (HISTORICAL)")
        print("="*80)
        
        predictions_test_df = test_df
        for target, model in trained_models.items():
            predictions_test_df = model.transform(predictions_test_df)
            
        evaluator = ForecastEvaluator()
        metrics = evaluator.evaluate_all_models(predictions_test_df)
        
        print("\n📊 Evaluation Summary:")
        for target, m in metrics.items():
            print(f"   - {target}: RMSE={m.get('rmse', 'N/A'):.4f}, R2={m.get('r2', 'N/A'):.4f}")

        # ==========================================
        # STEP 6.5: GENERATE FUTURE FORECAST (7 DAYS)
        # ==========================================
        print("\n" + "="*80)
        print("STEP 6.5: GENERATING FUTURE FORECAST")
        print("="*80)
        
        # Dùng df_features (toàn bộ dữ liệu) để lấy dòng cuối cùng làm mốc
        future_forecast_df = generate_future_forecast(spark, trained_models, df_features, days=7)

        # ==========================================
        # STEP 7: WRITE TO POSTGRESQL 
        # ==========================================
        print("\n" + "="*80)
        print("STEP 7: WRITING TO POSTGRESQL")
        print("="*80)

        # 1. Xác định cột cần ghi
        target_cols = list(config.CONTINUOUS_FEATURES) 
        if hasattr(config, 'CATEGORICAL_FEATURES'):
            target_cols += config.CATEGORICAL_FEATURES            
        prediction_cols = [f"prediction_{c}" for c in target_cols]
        
        # Cột cơ bản
        base_cols = ['datetime', 'city']
        
        # --- Xử lý DataFrame Test (Quá khứ) ---
        # Select: Time + City + Actual + Prediction
        select_cols_test = base_cols + \
                           [c for c in target_cols if c in predictions_test_df.columns] + \
                           [c for c in prediction_cols if c in predictions_test_df.columns]
        
        export_test_df = predictions_test_df.select(select_cols_test)
        
        # --- Xử lý DataFrame Future (Tương lai) ---
        # Future DF chỉ có cột Prediction. Ta cần thêm cột Actual (là Null) để union được
        if future_forecast_df:
            for col_name in target_cols:
                # Thêm cột Actual với giá trị Null (vì tương lai chưa xảy ra)
                future_forecast_df = future_forecast_df.withColumn(col_name, lit(None).cast("double"))
            
            # Select đúng thứ tự cột như Test DF
            # Lưu ý: future_forecast_df có thể thiếu một số cột nếu model không predict ra, cần check
            valid_future_cols = [c for c in select_cols_test if c in future_forecast_df.columns]
            export_future_df = future_forecast_df.select(valid_future_cols)
            
            # Union: Gộp Quá khứ + Tương lai
            final_export_df = export_test_df.unionByName(export_future_df, allowMissingColumns=True)
        else:
            final_export_df = export_test_df

        print(f"   Writing {final_export_df.count()} records (Historical + Future) to database...")
        
        # 2. Gọi Postgres Writer
        pg_writer = PostgresWriter()
        success = pg_writer.write_predictions_safe(final_export_df)
        
        if success:
            print("   ✅ Database update complete.")
        else:
            print("   ⚠️ Database update skipped/failed.")

        # ==========================================
        # STEP 8: EXPORT CSV & VISUALIZATION
        # ==========================================
        if not os.path.exists(LOCAL_OUTPUT_DIR):
            os.makedirs(LOCAL_OUTPUT_DIR)
            
        print(f"\n✅ Pipeline Complete.")
        
        print("\n" + "="*80)
        print("STEP 8: VISUALIZATION")
        print("="*80)

        try:
            viz = ForecastVisualizer()
            # Chuyển Spark DataFrame sang Pandas để vẽ (Chỉ làm khi dữ liệu < 100k dòng)
            pandas_df = predictions_test_df.toPandas()
            
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