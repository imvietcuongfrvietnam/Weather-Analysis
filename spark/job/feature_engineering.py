"""
Feature Engineering - Time Series Features for Weather Forecasting
Tạo các đặc trưng chuỗi thời gian cho dự đoán thời tiết
Updated: Removed precipitation_mm, Added wind_direction support
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lag, avg, stddev, sum as spark_sum, max as spark_max,
    hour, dayofweek, month, when, sin, cos, lit, abs
)
import math
import sys
import os

# Thêm đường dẫn để import config nếu cần
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import config
except ImportError:
    # Fallback config nếu không tìm thấy file
    class Config:
        LAG_HOURS = [1, 3, 6, 12, 24]
        ROLLING_WINDOWS = [6, 12, 24]
    config = Config()

class TimeSeriesFeatureEngineer:
    """Create time series features for weather forecasting"""
    
    # Danh sách các cột số liệu liên tục cần tạo Lag/Rolling
    # Đã xóa precipitation_mm
    CONTINUOUS_FEATURES = [
        "temperature", 
        "humidity", 
        "pressure", 
        "wind_speed", 
        "wind_direction"
    ]

    @staticmethod
    def create_lag_features(df: DataFrame, feature_cols: list, lag_hours: list) -> DataFrame:
        """
        Tạo các đặc trưng trễ (Lag features)
        Ví dụ: temperature_lag_1h là nhiệt độ của 1 giờ trước
        """
        print(f"\n🔄 Creating lag features for {len(feature_cols)} features...")
        
        # Cửa sổ trượt theo từng thành phố, sắp xếp theo thời gian
        window_spec = Window.partitionBy("city").orderBy("datetime")
        
        for feature in feature_cols:
            if feature not in df.columns:
                print(f"   ⚠️  Skipping {feature} (not in data)")
                continue
                
            for lag_h in lag_hours:
                lag_col_name = f"{feature}_lag_{lag_h}h"
                df = df.withColumn(
                    lag_col_name,
                    lag(col(feature), lag_h).over(window_spec)
                )
        
        return df
    
    @staticmethod
    def create_rolling_features(df: DataFrame, feature_cols: list, window_sizes: list) -> DataFrame:
        """
        Tạo các đặc trưng thống kê trượt (Rolling Window Statistics)
        Trung bình, độ lệch chuẩn, max, min trong khoảng thời gian
        """
        print(f"\n📊 Creating rolling window features...")
        
        window_spec_base = Window.partitionBy("city").orderBy("datetime")
        
        for feature in feature_cols:
            if feature not in df.columns:
                continue
            
            for window_h in window_sizes:
                # rowsBetween(-window_h, 0): Từ window_h dòng trước đến dòng hiện tại
                window_spec = window_spec_base.rowsBetween(-window_h, 0)
                
                # Rolling Mean (Trung bình trượt)
                df = df.withColumn(
                    f"{feature}_rolling_mean_{window_h}h",
                    avg(col(feature)).over(window_spec)
                )
                
                # Rolling Std Dev (Độ lệch chuẩn trượt - đo độ biến động)
                df = df.withColumn(
                    f"{feature}_rolling_std_{window_h}h",
                    stddev(col(feature)).over(window_spec)
                )
                
                # Với gió thì lấy thêm Max (Gió giật)
                if 'wind' in feature:
                    df = df.withColumn(
                        f"{feature}_rolling_max_{window_h}h",
                        spark_max(col(feature)).over(window_spec)
                    )
        
        return df
    
    @staticmethod
    def create_time_features(df: DataFrame) -> DataFrame:
        """
        Tạo đặc trưng thời gian (Hour, Day, Month, Season...)
        """
        print(f"\n⏰ Creating time-based features...")
        
        # 1. Basic Components
        df = df.withColumn("hour_of_day", hour(col("datetime")))
        df = df.withColumn("day_of_week", dayofweek(col("datetime")))  # 1=CN, 7=Thứ 7
        df = df.withColumn("month_of_year", month(col("datetime")))
        
        # 2. Season (Mùa)
        df = df.withColumn(
            "season",
            when(col("month_of_year").isin([12, 1, 2]), "winter")
            .when(col("month_of_year").isin([3, 4, 5]), "spring")
            .when(col("month_of_year").isin([6, 7, 8]), "summer")
            .otherwise("fall")
        )
        
        # 3. Cyclical Encoding (Quan trọng cho Model hiểu tính chu kỳ của giờ/ngày)
        # Hour: 0-23
        df = df.withColumn("hour_sin", sin(col("hour_of_day") * 2 * math.pi / 24))
        df = df.withColumn("hour_cos", cos(col("hour_of_day") * 2 * math.pi / 24))
        
        # Month: 1-12
        df = df.withColumn("month_sin", sin((col("month_of_year") - 1) * 2 * math.pi / 12))
        df = df.withColumn("month_cos", cos((col("month_of_year") - 1) * 2 * math.pi / 12))
        
        print("   ✅ Time features created: hour, day, month, season, cyclical encodings")
        
        return df
    
    @staticmethod
    def create_derived_features(df: DataFrame) -> DataFrame:
        """
        Tạo các đặc trưng khí tượng học phái sinh (Derived Features)
        """
        print(f"\n🌡️  Creating derived meteorological features...")
        
        # 1. Temperature Change (Biến thiên nhiệt độ so với 1h trước)
        if 'temperature' in df.columns and 'temperature_lag_1h' in df.columns:
            df = df.withColumn(
                "temp_change_1h",
                col("temperature") - col("temperature_lag_1h")
            )
        
        # 2. Pressure Tendency (Xu hướng áp suất - Dự báo bão/mưa)
        if 'pressure' in df.columns and 'pressure_lag_3h' in df.columns:
            df = df.withColumn(
                "pressure_tendency",
                when(col("pressure") - col("pressure_lag_3h") > 2, "rising")
                .when(col("pressure") - col("pressure_lag_3h") < -2, "falling")
                .otherwise("stable")
            )
        
        # 3. Heat Index (Chỉ số nóng bức - Feels Like)
        if 'temperature' in df.columns and 'humidity' in df.columns:
            df = df.withColumn(
                "heat_index",
                col("temperature") + 
                (0.05 * col("humidity") * (col("temperature") - 20))
            )
        
        # 4. Wind Chill (Chỉ số rét run)
        if 'temperature' in df.columns and 'wind_speed' in df.columns:
            df = df.withColumn(
                "wind_chill",
                when(
                    (col("temperature") < 10) & (col("wind_speed") > 5),
                    col("temperature") - (0.4 * col("wind_speed"))
                ).otherwise(col("temperature"))
            )
            
        print("   ✅ Derived features: temp_change, pressure_tendency, heat_index, wind_chill")
        
        return df
    
    @staticmethod
    def engineer_all_features(df: DataFrame) -> DataFrame:
        """
        Hàm chính: Chạy toàn bộ quy trình tạo đặc trưng
        """
        print("\n" + "="*60)
        print("🔧 FEATURE ENGINEERING PIPELINE")
        print("="*60)
        
        # Đảm bảo dữ liệu sắp xếp theo thời gian
        df = df.orderBy("datetime")
        
        # 1. Time Features
        df = TimeSeriesFeatureEngineer.create_time_features(df)
        
        # 2. Lag Features (Dùng danh sách cột chuẩn, loại bỏ cột không có)
        cols_to_lag = [c for c in TimeSeriesFeatureEngineer.CONTINUOUS_FEATURES if c in df.columns]
        df = TimeSeriesFeatureEngineer.create_lag_features(
            df,
            cols_to_lag,
            config.LAG_HOURS
        )
        
        # 3. Rolling Features
        df = TimeSeriesFeatureEngineer.create_rolling_features(
            df,
            cols_to_lag,
            config.ROLLING_WINDOWS
        )
        
        # 4. Derived Features
        df = TimeSeriesFeatureEngineer.create_derived_features(df)
        
        print("\n✅ Feature engineering complete!")
        print(f"   Total columns: {len(df.columns)}")
        print("="*60 + "\n")
        
        return df
    
    @staticmethod
    def get_feature_columns(df: DataFrame, exclude_targets: bool = True) -> list:
        """
        Lấy danh sách các cột dùng để Train Model (bỏ cột ID, Time, Target)
        """
        # Các cột định danh/metadata không dùng để train
        exclude_cols = ['datetime', 'city', 'weather_desc', 'weather_desc_clean']
        
        # Các cột Target (Biến mục tiêu cần dự đoán) - Đã xóa precipitation_mm
        targets = ["temperature", "humidity", "pressure", "wind_speed", "wind_direction"]
        
        if exclude_targets:
            exclude_cols.extend(targets)
        
        # Các cột String chưa encode cũng bỏ qua (chỉ lấy số)
        exclude_cols.extend(['season', 'pressure_tendency', 'precip_intensity'])
        
        # Lọc lấy các cột số còn lại có trong DataFrame
        feature_cols = [
            field.name for field in df.schema.fields
            if field.name not in exclude_cols
            and field.dataType.simpleString() in ['double', 'int', 'bigint', 'float']
        ]
        
        return feature_cols