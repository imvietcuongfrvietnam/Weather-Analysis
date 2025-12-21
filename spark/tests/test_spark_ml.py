"""
TEST SUITE: Weather Forecasting ML Pipeline
Vị trí: spark/tests/test_spark_ml.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from datetime import datetime, timedelta
import os
import shutil
import sys

# --- 1. SETUP PATH ---
# Trỏ ngược ra thư mục 'job' để import được modules
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
job_dir = os.path.join(parent_dir, 'job')

if job_dir not in sys.path:
    sys.path.append(job_dir)

try:
    import config
    from feature_engineering import TimeSeriesFeatureEngineer
    from models import WeatherForecastModels
    from forecast_evaluator import ForecastEvaluator
    
    # --- QUAN TRỌNG: GHI ĐÈ CONFIG ĐỂ KHỚP VỚI MOCK DATA ---
    print("🔧 Overriding Config for Testing...")
    # Cập nhật tên cột chuẩn mới (Khớp với Mock Data bên dưới)
    config.CONTINUOUS_FEATURES = ["temperature", "humidity", "pressure", "wind_speed", "precipitation_mm"]
    config.CATEGORICAL_FEATURES = ["weather_desc"] 
    
    # Cập nhật mapping model
    config.MODEL_SELECTION = {
        "temperature": "GBT",
        "humidity": "GBT", 
        "pressure": "GBT",
        "wind_speed": "RandomForest",
        "precipitation_mm": "RandomForest"
    }
    # -------------------------------------------------------

except ImportError as e:
    print(f"❌ Lỗi Import: {e}")
    sys.exit(1)

def create_local_test_spark():
    return SparkSession.builder \
        .appName("WeatherML_Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def generate_mock_data(spark, num_rows=500):
    """
    Sinh dữ liệu giả lập (Mock Data)
    QUAN TRỌNG: Tên cột phải khớp với config đã override ở trên
    """
    print(f"🎲 Generating {num_rows} rows of mock data...")
    
    data = []
    base_time = datetime.now() - timedelta(hours=num_rows)
    cities = ["Hanoi", "HoChiMinh", "DaNang"]
    weather_types = ["rain", "clouds", "clear", "snow"]

    for i in range(num_rows):
        current_time = base_time + timedelta(hours=i)
        
        row = (
            current_time,
            random.choice(cities),
            25.0 + random.uniform(-5, 5),     # temperature
            70.0 + random.uniform(-20, 20),   # humidity
            1013.0 + random.uniform(-10, 10), # pressure
            15.0 + random.uniform(0, 20),     # wind_speed
            random.uniform(0, 360),           # wind_direction
            random.choice(weather_types),     # weather_desc
            0.0 if random.random() > 0.3 else random.uniform(0, 10) # precipitation_mm
        )
        data.append(row)

    # SCHEMA CHUẨN (Khớp với config override)
    schema = StructType([
        StructField("datetime", TimestampType(), False),
        StructField("city", StringType(), False),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("weather_desc", StringType(), True),
        StructField("precipitation_mm", DoubleType(), True)
    ])

    return spark.createDataFrame(data, schema)

def test_pipeline():
    print("\n" + "="*60)
    print("🧪 STARTING INTEGRATION TEST (ML PIPELINE)")
    print("="*60)

    spark = create_local_test_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 2. Load Mock Data
        df = generate_mock_data(spark)
        print("✅ Mock Data generated successfully.")
        
        # 3. Test Feature Engineering
        print("\n⚙️  Testing Feature Engineering...")
        engineer = TimeSeriesFeatureEngineer()
        
        # Tạo features
        df_features = engineer.engineer_all_features(df)
        
        # Lấy danh sách cột Feature
        feature_cols = engineer.get_feature_columns(df_features, exclude_targets=True)
        
        if len(feature_cols) > 0:
            print(f"✅ Feature Engineering OK. Created {len(feature_cols)} features.")
            lag_cols = [c for c in df_features.columns if "lag" in c]
            if lag_cols:
                print(f"   (Lag features detected: {len(lag_cols)})")
        else:
            print("❌ Feature Engineering Failed: No features created.")
            return

        # 4. Splitting Data
        df_clean = df_features.dropna()
        train_df, test_df = df_clean.randomSplit([0.7, 0.3], seed=42)
        print(f"📊 Data Size - Raw: {df.count()}, Cleaned: {df_clean.count()}")
        print(f"📊 Split - Train: {train_df.count()}, Test: {test_df.count()}")

        if train_df.count() == 0:
            print("❌ Error: Training set empty. Có thể do tạo quá nhiều Lag feature trên dữ liệu quá ít.")
            return

        # 5. Test Model Building & Training
        print("\n🧠 Testing Model Training...")
        model_builder = WeatherForecastModels()
        
        # --- BƯỚC BUILD MODEL (QUAN TRỌNG) ---
        print("   🔨 Building model pipelines...")
        model_builder.build_all_models(feature_cols) 
        # -------------------------------------

        print("   🏋️  Training models (this may take a moment)...")
        trained_models = model_builder.train_all_models(train_df)
        
        if len(trained_models) > 0:
            print(f"✅ Training OK. Trained {len(trained_models)} models.")
        else:
            print("❌ Training Failed: No models trained.")
            return

        # 6. Test Prediction
        print("\n🔮 Testing Prediction...")
        predictions_df = test_df
        for target, model in trained_models.items():
            predictions_df = model.transform(predictions_df)
        
        pred_cols = [c for c in predictions_df.columns if "prediction" in c]
        print(f"✅ Prediction OK. Prediction Columns: {pred_cols}")

        # 7. Test Evaluation
        print("\n📉 Testing Evaluation...")
        evaluator = ForecastEvaluator()
        metrics = evaluator.evaluate_all_models(predictions_df)
        
        print(f"✅ Evaluation OK. Metrics calculated for {len(metrics)} targets.")
        if metrics:
            first_target = list(metrics.keys())[0]
            print(f"   Sample ({first_target}): RMSE = {metrics[first_target].get('rmse', 'N/A')}")

        # 8. Test Saving Models
        test_model_dir = "./test_models_temp"
        if os.path.exists(test_model_dir):
            shutil.rmtree(test_model_dir)
        
        print(f"\n💾 Testing Model Save...")
        model_builder.save_all_models(trained_models, test_model_dir)
        
        if os.path.exists(test_model_dir) and len(os.listdir(test_model_dir)) > 0:
             print(f"✅ Model Saving OK. Saved to {test_model_dir}")
             shutil.rmtree(test_model_dir)
        else:
             print("❌ Model Saving Failed.")

        print("\n" + "="*60)
        print("🎉 TEST COMPLETED SUCCESSFULLY!")
        print("="*60)

    except Exception as e:
        print(f"\n❌ TEST FAILED WITH ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    test_pipeline()