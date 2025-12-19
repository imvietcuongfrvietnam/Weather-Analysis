"""
Spark ML với LSTM để dự đoán thời tiết 7 ngày tới
Đọc dữ liệu từ MinIO, huấn luyện LSTM, và ghi kết quả vào PostgreSQL

CHẠY:
    python spark_lstm_forecast.py
    hoặc
    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0 spark_lstm_forecast.py
"""

import sys
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# Thêm đường dẫn để import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'spark_etl_weather_disaster'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql import functions as F

# Import configs
import minio_config
import postgres_config

# TensorFlow/Keras cho LSTM
try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    from tensorflow.keras.optimizers import Adam
    from sklearn.preprocessing import MinMaxScaler
    TF_AVAILABLE = True
except ImportError:
    print("⚠️  TensorFlow không được cài đặt. Cài đặt bằng: pip install tensorflow")
    TF_AVAILABLE = False


# ===========================
# CẤU HÌNH
# ===========================

# Số ngày để dự đoán
FORECAST_DAYS = 7

# Số ngày lịch sử để huấn luyện (window size)
LOOKBACK_DAYS = 30

# Các features để dự đoán
FEATURE_COLUMNS = ['temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction']

# Batch size cho LSTM
BATCH_SIZE = 32
EPOCHS = 50


# ===========================
# KHỞI TẠO SPARK SESSION
# ===========================

def create_spark_session():
    """Khởi tạo Spark với hỗ trợ S3/MinIO và PostgreSQL"""
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.5.0"
    ]
    
    builder = SparkSession.builder \
        .appName("WeatherLSTMForecast") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", ",".join(packages))

    # Cấu hình MinIO S3
    print("📦 Đang nạp cấu hình MinIO S3...")
    for key, value in minio_config.SPARK_S3_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ===========================
# ĐỌC DỮ LIỆU TỪ MINIO
# ===========================

def read_data_from_minio(spark):
    """
    Đọc dữ liệu thời tiết từ MinIO
    
    Returns:
        DataFrame: Spark DataFrame chứa dữ liệu thời tiết
    """
    print("\n" + "="*80)
    print("📥 ĐỌC DỮ LIỆU TỪ MINIO")
    print("="*80)
    
    # Đường dẫn đến dữ liệu enriched trong MinIO
    minio_path = minio_config.get_minio_path("enriched", "weather", format="parquet")
    
    print(f"📁 Đường dẫn MinIO: {minio_path}")
    
    try:
        # Đọc Parquet từ MinIO
        df = spark.read.parquet(minio_path)
        
        # Kiểm tra dữ liệu
        record_count = df.count()
        print(f"✅ Đã đọc {record_count:,} bản ghi từ MinIO")
        
        if record_count == 0:
            raise ValueError("Không có dữ liệu trong MinIO! Vui lòng chạy ETL pipeline trước.")
        
        # Hiển thị schema
        print("\n📊 Schema dữ liệu:")
        df.printSchema()
        
        # Hiển thị sample
        print("\n📋 Sample dữ liệu (5 dòng đầu):")
        df.select("datetime", "city", "temperature", "humidity", "pressure", "wind_speed").show(5, truncate=False)
        
        return df
        
    except Exception as e:
        print(f"❌ Lỗi đọc dữ liệu từ MinIO: {e}")
        print(f"💡 Kiểm tra:")
        print(f"   1. MinIO server đang chạy")
        print(f"   2. Đường dẫn: {minio_path}")
        print(f"   3. Bucket và folder tồn tại")
        raise


# ===========================
# CHUẨN BỊ DỮ LIỆU CHO TIME SERIES
# ===========================

def prepare_time_series_data(df, city=None):
    """
    Chuẩn bị dữ liệu time series cho một thành phố
    
    Args:
        df: Spark DataFrame
        city: Tên thành phố (None nếu muốn xử lý tất cả)
    
    Returns:
        pandas.DataFrame: Dữ liệu đã được sắp xếp theo thời gian
    """
    print(f"\n⚙️  Chuẩn bị dữ liệu time series cho {city if city else 'tất cả các thành phố'}...")
    
    # Lọc theo thành phố nếu có
    if city:
        df_city = df.filter(col("city") == city)
    else:
        df_city = df
    
    # Chọn các cột cần thiết và sắp xếp theo thời gian
    df_prepared = df_city.select(
        "datetime",
        "city",
        "temperature",
        "humidity", 
        "pressure",
        "wind_speed",
        "wind_direction"
    ).filter(
        col("temperature").isNotNull() &
        col("humidity").isNotNull() &
        col("pressure").isNotNull() &
        col("wind_speed").isNotNull()
    ).orderBy("datetime")
    
    # Chuyển sang Pandas để xử lý LSTM
    pandas_df = df_prepared.toPandas()
    
    if len(pandas_df) == 0:
        raise ValueError(f"Không có dữ liệu cho thành phố {city}")
    
    # Đảm bảo datetime là datetime type
    pandas_df['datetime'] = pd.to_datetime(pandas_df['datetime'])
    
    # Sắp xếp lại theo thời gian
    pandas_df = pandas_df.sort_values('datetime').reset_index(drop=True)
    
    # Loại bỏ duplicates theo datetime
    pandas_df = pandas_df.drop_duplicates(subset=['datetime'], keep='last')
    
    print(f"✅ Đã chuẩn bị {len(pandas_df)} bản ghi")
    print(f"   Thời gian: {pandas_df['datetime'].min()} đến {pandas_df['datetime'].max()}")
    
    return pandas_df


# ===========================
# XÂY DỰNG VÀ HUẤN LUYỆN LSTM
# ===========================

def create_sequences(data, lookback=LOOKBACK_DAYS):
    """
    Tạo sequences cho LSTM
    
    Args:
        data: numpy array với shape (n_samples, n_features)
        lookback: Số timesteps trong quá khứ để dự đoán
    
    Returns:
        X, y: Input sequences và target values
    """
    X, y = [], []
    for i in range(lookback, len(data)):
        X.append(data[i-lookback:i])
        y.append(data[i])
    return np.array(X), np.array(y)


def build_lstm_model(input_shape, n_features):
    """
    Xây dựng mô hình LSTM
    
    Args:
        input_shape: Shape của input (lookback, n_features)
        n_features: Số lượng features
    
    Returns:
        keras.Model: Mô hình LSTM
    """
    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=input_shape),
        Dropout(0.2),
        LSTM(50, return_sequences=True),
        Dropout(0.2),
        LSTM(50),
        Dropout(0.2),
        Dense(n_features)  # Dự đoán tất cả features cùng lúc
    ])
    
    model.compile(
        optimizer=Adam(learning_rate=0.001),
        loss='mse',
        metrics=['mae']
    )
    
    return model


def train_lstm_model(pandas_df, city_name):
    """
    Huấn luyện mô hình LSTM cho một thành phố
    
    Args:
        pandas_df: DataFrame chứa dữ liệu time series
        city_name: Tên thành phố
    
    Returns:
        model: Trained LSTM model
        scaler: MinMaxScaler đã fit
    """
    print(f"\n🧠 Huấn luyện LSTM cho {city_name}...")
    
    if not TF_AVAILABLE:
        raise ImportError("TensorFlow không được cài đặt!")
    
    # Chọn features
    feature_data = pandas_df[FEATURE_COLUMNS].values
    
    # Chuẩn hóa dữ liệu
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(feature_data)
    
    # Tạo sequences
    lookback = LOOKBACK_DAYS * 24  # Giả sử dữ liệu theo giờ (24 giờ/ngày)
    if len(scaled_data) < lookback + FORECAST_DAYS * 24:
        lookback = max(7 * 24, len(scaled_data) // 4)  # Điều chỉnh nếu không đủ dữ liệu
        print(f"   ⚠️  Điều chỉnh lookback thành {lookback} timesteps")
    
    X, y = create_sequences(scaled_data, lookback)
    
    if len(X) == 0:
        raise ValueError(f"Không đủ dữ liệu để tạo sequences. Cần ít nhất {lookback + 1} timesteps")
    
    # Chia train/test
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]
    
    print(f"   📊 Training samples: {len(X_train)}")
    print(f"   📊 Test samples: {len(X_test)}")
    
    # Xây dựng mô hình
    input_shape = (X_train.shape[1], X_train.shape[2])
    model = build_lstm_model(input_shape, len(FEATURE_COLUMNS))
    
    print(f"   🏗️  Mô hình LSTM:")
    model.summary()
    
    # Huấn luyện
    print(f"   🚀 Bắt đầu huấn luyện ({EPOCHS} epochs)...")
    history = model.fit(
        X_train, y_train,
        batch_size=BATCH_SIZE,
        epochs=EPOCHS,
        validation_data=(X_test, y_test),
        verbose=1
    )
    
    # Đánh giá
    train_loss = model.evaluate(X_train, y_train, verbose=0)
    test_loss = model.evaluate(X_test, y_test, verbose=0)
    
    print(f"\n   ✅ Huấn luyện hoàn tất!")
    print(f"   📈 Train Loss: {train_loss[0]:.4f}, Train MAE: {train_loss[1]:.4f}")
    print(f"   📈 Test Loss: {test_loss[0]:.4f}, Test MAE: {test_loss[1]:.4f}")
    
    return model, scaler, lookback


# ===========================
# DỰ ĐOÁN 7 NGÀY TỚI
# ===========================

def forecast_next_7_days(model, scaler, pandas_df, lookback, city_name):
    """
    Dự đoán thời tiết 7 ngày tới
    
    Args:
        model: Trained LSTM model
        scaler: MinMaxScaler
        pandas_df: Historical data
        lookback: Lookback window size
        city_name: Tên thành phố
    
    Returns:
        pandas.DataFrame: DataFrame chứa dự đoán
    """
    print(f"\n🔮 Dự đoán thời tiết 7 ngày tới cho {city_name}...")
    
    # Lấy dữ liệu cuối cùng để làm input
    feature_data = pandas_df[FEATURE_COLUMNS].values
    scaled_data = scaler.transform(feature_data)
    
    # Lấy sequence cuối cùng
    last_sequence = scaled_data[-lookback:].reshape(1, lookback, len(FEATURE_COLUMNS))
    
    # Dự đoán từng bước (hourly)
    predictions = []
    current_input = last_sequence.copy()
    
    # Dự đoán 7 ngày = 7 * 24 = 168 giờ
    forecast_hours = FORECAST_DAYS * 24
    
    for i in range(forecast_hours):
        # Dự đoán timestep tiếp theo
        next_pred = model.predict(current_input, verbose=0)
        predictions.append(next_pred[0])
        
        # Cập nhật input cho lần dự đoán tiếp theo
        # Thêm prediction vào cuối và bỏ đầu
        current_input = np.append(current_input[:, 1:, :], next_pred.reshape(1, 1, -1), axis=1)
    
    # Chuyển đổi predictions về scale gốc
    predictions_array = np.array(predictions)
    predictions_original = scaler.inverse_transform(predictions_array)
    
    # Tạo DataFrame với dự đoán
    last_datetime = pd.to_datetime(pandas_df['datetime'].max())
    forecast_dates = pd.date_range(
        start=last_datetime + timedelta(hours=1),
        periods=forecast_hours,
        freq='H'
    )
    
    # Xử lý wind_direction nếu không có trong features
    wind_dir_values = predictions_original[:, 4] if len(FEATURE_COLUMNS) > 4 else np.zeros(len(predictions_original))
    
    forecast_df = pd.DataFrame({
        'city': city_name,
        'forecast_datetime': forecast_dates,
        'forecast_date': forecast_dates.date,  # Sẽ được convert sau
        'temperature': predictions_original[:, 0],
        'humidity': predictions_original[:, 1],
        'pressure': predictions_original[:, 2],
        'wind_speed': predictions_original[:, 3],
        'wind_direction': wind_dir_values,
        'model_version': 'LSTM_v1.0',
        'prediction_timestamp': pd.Timestamp.now(),
        'confidence_score': 0.85  # Có thể tính toán dựa trên validation loss
    })
    
    # Đảm bảo forecast_date là date type (không phải datetime)
    forecast_df['forecast_date'] = pd.to_datetime(forecast_df['forecast_datetime']).dt.date
    
    print(f"✅ Đã tạo {len(forecast_df)} dự đoán")
    print(f"   Từ {forecast_dates[0]} đến {forecast_dates[-1]}")
    
    return forecast_df


# ===========================
# GHI VÀO POSTGRESQL
# ===========================

def write_forecasts_to_postgres(spark, forecast_df):
    """
    Ghi kết quả dự đoán vào PostgreSQL
    
    Args:
        spark: SparkSession
        forecast_df: pandas.DataFrame chứa dự đoán
    """
    print("\n" + "="*80)
    print("💾 GHI KẾT QUẢ VÀO POSTGRESQL")
    print("="*80)
    
    # Chuyển pandas DataFrame sang Spark DataFrame
    spark_df = spark.createDataFrame(forecast_df)
    
    # Đổi tên cột để khớp với schema PostgreSQL
    spark_df = spark_df.select(
        col("city").alias("city"),
        col("forecast_date").alias("forecast_date"),
        col("forecast_datetime").alias("forecast_datetime"),
        col("temperature").alias("temperature_celsius"),
        col("humidity").alias("humidity_pct"),
        col("pressure").alias("pressure_hpa"),
        col("wind_speed").alias("wind_speed_kmh"),
        col("wind_direction").alias("wind_direction_deg"),
        col("model_version").alias("model_version"),
        col("prediction_timestamp").alias("prediction_timestamp"),
        col("confidence_score").alias("confidence_score")
    )
    
    print(f"📊 Số lượng dự đoán: {spark_df.count()}")
    print("\n📋 Sample dữ liệu sẽ ghi:")
    spark_df.show(10, truncate=False)
    
    # Ghi vào PostgreSQL
    try:
        jdbc_config = postgres_config.get_spark_jdbc_config()
        jdbc_props = postgres_config.get_spark_jdbc_properties()
        
        print(f"\n🔗 Kết nối PostgreSQL:")
        print(f"   URL: {jdbc_config['url']}")
        print(f"   Table: {postgres_config.FORECAST_TABLE}")
        
        # Ghi với mode append (có thể có duplicates, sẽ xử lý bằng UNIQUE constraint)
        spark_df.write \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", postgres_config.FORECAST_TABLE) \
            .option("user", jdbc_props['user']) \
            .option("password", jdbc_props['password']) \
            .option("driver", jdbc_props['driver']) \
            .mode("append") \
            .save()
        
        print("✅ Đã ghi thành công vào PostgreSQL!")
        
    except Exception as e:
        print(f"❌ Lỗi ghi vào PostgreSQL: {e}")
        print(f"💡 Kiểm tra:")
        print(f"   1. PostgreSQL server đang chạy")
        print(f"   2. Database và table đã được tạo")
        print(f"   3. Credentials đúng")
        raise


# ===========================
# MAIN PIPELINE
# ===========================

def main():
    """Hàm main để chạy toàn bộ pipeline"""
    print("\n" + "="*80)
    print("🚀 SPARK ML - LSTM FORECAST PIPELINE")
    print("="*80)
    print(f"📅 Dự đoán {FORECAST_DAYS} ngày tới")
    print(f"📊 Lookback: {LOOKBACK_DAYS} ngày")
    print("="*80)
    
    if not TF_AVAILABLE:
        print("\n❌ TensorFlow không được cài đặt!")
        print("💡 Cài đặt: pip install tensorflow")
        sys.exit(1)
    
    # Khởi tạo Spark
    spark = create_spark_session()
    
    try:
        # 1. Đọc dữ liệu từ MinIO
        df = read_data_from_minio(spark)
        
        # 2. Lấy danh sách các thành phố
        cities = df.select("city").distinct().rdd.map(lambda r: r[0]).collect()
        print(f"\n🏙️  Tìm thấy {len(cities)} thành phố: {cities[:5]}...")
        
        # 3. Xử lý từng thành phố (hoặc chỉ thành phố đầu tiên để demo)
        all_forecasts = []
        
        # Chỉ xử lý 3 thành phố đầu tiên để demo (có thể thay đổi)
        cities_to_process = cities[:3] if len(cities) > 3 else cities
        
        for city in cities_to_process:
            try:
                print(f"\n{'='*80}")
                print(f"🏙️  XỬ LÝ THÀNH PHỐ: {city}")
                print(f"{'='*80}")
                
                # Chuẩn bị dữ liệu
                city_df = prepare_time_series_data(df, city)
                
                # Huấn luyện LSTM
                model, scaler, lookback = train_lstm_model(city_df, city)
                
                # Dự đoán
                forecast_df = forecast_next_7_days(model, scaler, city_df, lookback, city)
                
                all_forecasts.append(forecast_df)
                
            except Exception as e:
                print(f"❌ Lỗi xử lý thành phố {city}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # 4. Gộp tất cả dự đoán và ghi vào PostgreSQL
        if all_forecasts:
            combined_forecasts = pd.concat(all_forecasts, ignore_index=True)
            write_forecasts_to_postgres(spark, combined_forecasts)
            
            print("\n" + "="*80)
            print("✅ HOÀN TẤT PIPELINE!")
            print("="*80)
            print(f"📊 Tổng số dự đoán: {len(combined_forecasts)}")
            print(f"🏙️  Số thành phố: {len(cities_to_process)}")
            print(f"📅 Dự đoán từ {combined_forecasts['forecast_datetime'].min()} đến {combined_forecasts['forecast_datetime'].max()}")
        else:
            print("\n❌ Không có dự đoán nào được tạo!")
        
    except Exception as e:
        print(f"\n❌ LỖI: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
        print("\n👋 Đã dừng Spark Session")


if __name__ == "__main__":
    main()

