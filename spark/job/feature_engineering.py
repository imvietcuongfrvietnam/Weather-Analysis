"""
Feature Engineering - Time Series Features for Weather Forecasting
Tạo các đặc trưng chuỗi thời gian cho dự đoán thời tiết
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lag, avg, stddev, sum as spark_sum, max as spark_max,
    hour, dayofweek, month, when, sin, cos, lit, expr
)
import math
import config

class TimeSeriesFeatureEngineer:
    """Create time series features for weather forecasting"""
    
    @staticmethod
    def create_lag_features(df: DataFrame, feature_cols: list, lag_hours: list) -> DataFrame:
        """
        Create lag features (past values) for time series
        
        Args:
            df: Input DataFrame sorted by datetime
            feature_cols: List of features to create lags for
            lag_hours: List of lag intervals in hours
            
        Returns:
            DataFrame with lag features added
        """
        print(f"\n🔄 Creating lag features for {len(feature_cols)} features...")
        
        # Define window spec partitioned by city, ordered by datetime
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
            
            print(f"   ✅ {feature}: Created lags {lag_hours}")
        
        return df
    
    @staticmethod
    def create_rolling_features(df: DataFrame, feature_cols: list, window_sizes: list) -> DataFrame:
        """
        Create rolling window statistics (mean, std, sum, max)
        
        Args:
            df: Input DataFrame
            feature_cols: Features to calculate rolling stats for
            window_sizes: Window sizes in hours
            
        Returns:
            DataFrame with rolling features
        """
        print(f"\n📊 Creating rolling window features...")
        
        window_spec_base = Window.partitionBy("city").orderBy("datetime")
        
        for feature in feature_cols:
            if feature not in df.columns:
                continue
            
            for window_h in window_sizes:
                # Create window spec for this window size
                # rowsBetween(-window_h, 0) means from window_h rows back to current
                window_spec = window_spec_base.rowsBetween(-window_h, 0)
                
                # Rolling mean
                df = df.withColumn(
                    f"{feature}_rolling_mean_{window_h}h",
                    avg(col(feature)).over(window_spec)
                )
                
                # Rolling std
                df = df.withColumn(
                    f"{feature}_rolling_std_{window_h}h",
                    stddev(col(feature)).over(window_spec)
                )
                
                # For precipitation, also add sum
                if 'precipitation' in feature:
                    df = df.withColumn(
                        f"{feature}_rolling_sum_{window_h}h",
                        spark_sum(col(feature)).over(window_spec)
                    )
                
                # For wind, also add max
                if 'wind' in feature:
                    df = df.withColumn(
                        f"{feature}_rolling_max_{window_h}h",
                        spark_max(col(feature)).over(window_spec)
                    )
            
            print(f"   ✅ {feature}: Windows {window_sizes}h")
        
        return df
    
    @staticmethod
    def create_time_features(df: DataFrame) -> DataFrame:
        """
        Create time-based features from datetime
        
        Args:
            df: DataFrame with 'datetime' column
            
        Returns:
            DataFrame with time features
        """
        print(f"\n⏰ Creating time-based features...")
        
        # Extract basic time components
        df = df.withColumn("hour_of_day", hour(col("datetime")))
        df = df.withColumn("day_of_week", dayofweek(col("datetime")))  # 1=Sunday, 7=Saturday
        df = df.withColumn("month_of_year", month(col("datetime")))
        
        # Season
        df = df.withColumn(
            "season",
            when(col("month_of_year").isin([12, 1, 2]), "winter")
            .when(col("month_of_year").isin([3, 4, 5]), "spring")
            .when(col("month_of_year").isin([6, 7, 8]), "summer")
            .otherwise("fall")
        )
        
        # Binary features
        df = df.withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
        df = df.withColumn(
            "is_rush_hour",
            when(
                ((col("hour_of_day") >= 7) & (col("hour_of_day") <= 9)) |
                ((col("hour_of_day") >= 17) & (col("hour_of_day") <= 19)),
                1
            ).otherwise(0)
        )
        df = df.withColumn("is_night", when((col("hour_of_day") >= 20) | (col("hour_of_day") <= 6), 1).otherwise(0))
        
        # Cyclical encoding (to capture circular nature of time)
        # Hour: 0-23 -> sin/cos
        df = df.withColumn("hour_sin", sin(col("hour_of_day") * 2 * math.pi / 24))
        df = df.withColumn("hour_cos", cos(col("hour_of_day") * 2 * math.pi / 24))
        
        # Day of week: 1-7 -> sin/cos
        df = df.withColumn("dow_sin", sin((col("day_of_week") - 1) * 2 * math.pi / 7))
        df = df.withColumn("dow_cos", cos((col("day_of_week") - 1) * 2 * math.pi / 7))
        
        # Month: 1-12 -> sin/cos
        df = df.withColumn("month_sin", sin((col("month_of_year") - 1) * 2 * math.pi / 12))
        df = df.withColumn("month_cos", cos((col("month_of_year") - 1) * 2 * math.pi / 12))
        
        print("   ✅ Time features created: hour, day, month, season, cyclical encodings")
        
        return df
    
    @staticmethod
    def create_derived_features(df: DataFrame) -> DataFrame:
        """
        Create derived meteorological features
        
        Args:
            df: DataFrame with base weather features
            
        Returns:
            DataFrame with derived features
        """
        print(f"\n🌡️  Creating derived meteorological features...")
        
        # Temperature change rate (compared to 1 hour ago)
        if 'temp_celsius_lag_1h' in df.columns:
            df = df.withColumn(
                "temp_change_1h",
                col("temp_celsius") - col("temp_celsius_lag_1h")
            )
        
        # Pressure tendency (rising/falling/stable)
        if 'pressure_hpa_lag_3h' in df.columns:
            df = df.withColumn(
                "pressure_tendency",
                when(col("pressure_hpa") - col("pressure_hpa_lag_3h") > 2, "rising")
                .when(col("pressure_hpa") - col("pressure_hpa_lag_3h") < -2, "falling")
                .otherwise("stable")
            )
        
        # Humidity comfort (deviation from ideal ~50%)
        if 'humidity_pct' in df.columns:
            df = df.withColumn(
                "humidity_comfort",
                100 - abs(col("humidity_pct") - 50)
            )
        
        # Heat index approximation (feels-like temperature)
        if 'temp_celsius' in df.columns and 'humidity_pct' in df.columns:
            # Simplified heat index
            df = df.withColumn(
                "heat_index",
                col("temp_celsius") + 
                (0.05 * col("humidity_pct") * (col("temp_celsius") - 20))
            )
        
        # Wind chill approximation
        if 'temp_celsius' in df.columns and 'wind_speed_kmh' in df.columns:
            # Simplified wind chill (only for temps below 10°C and wind > 5km/h)
            df = df.withColumn(
                "wind_chill",
                when(
                    (col("temp_celsius") < 10) & (col("wind_speed_kmh") > 5),
                    col("temp_celsius") - (0.4 * col("wind_speed_kmh"))
                ).otherwise(col("temp_celsius"))
            )
        
        # Precipitation intensity category
        if 'precipitation_mm' in df.columns:
            df = df.withColumn(
                "precip_intensity",
                when(col("precipitation_mm") == 0, "none")
                .when(col("precipitation_mm") < 2.5, "light")
                .when(col("precipitation_mm") < 10, "moderate")
                .when(col("precipitation_mm") < 50, "heavy")
                .otherwise("extreme")
            )
        
        print("   ✅ Derived features: temp_change, pressure_tendency, heat_index, wind_chill, etc.")
        
        return df
    
    @staticmethod
    def engineer_all_features(df: DataFrame) -> DataFrame:
        """
        Apply all feature engineering steps
        
        Args:
            df: Raw DataFrame with base weather data
            
        Returns:
            DataFrame with all engineered features
        """
        print("\n" + "="*80)
        print("🔧 FEATURE ENGINEERING PIPELINE")
        print("="*80)
        
        # Make sure data is sorted by datetime
        df = df.orderBy("datetime")
        
        # 1. Time features (no dependencies)
        df = TimeSeriesFeatureEngineer.create_time_features(df)
        
        # 2. Lag features
        df = TimeSeriesFeatureEngineer.create_lag_features(
            df,
            config.CONTINUOUS_FEATURES,
            config.LAG_HOURS
        )
        
        # 3. Rolling window features
        df = TimeSeriesFeatureEngineer.create_rolling_features(
            df,
            config.CONTINUOUS_FEATURES,
            config.ROLLING_WINDOWS
        )
        
        # 4. Derived features (depend on lags)
        df = TimeSeriesFeatureEngineer.create_derived_features(df)
        
        # 5. Cache the result (important for iterative operations)
        df = df.cache()
        
        print("\n✅ Feature engineering complete!")
        print(f"   Total columns: {len(df.columns)}")
        print("="*80 + "\n")
        
        return df
    
    @staticmethod
    def get_feature_columns(df: DataFrame, exclude_targets: bool = True) -> list:
        """
        Get list of feature columns (excluding target and metadata columns)
        
        Args:
            df: DataFrame with engineered features
            exclude_targets: Whether to exclude target columns
            
        Returns:
            List of feature column names
        """
        # Columns to exclude from features
        exclude_cols = ['datetime', 'city']
        
        if exclude_targets:
            exclude_cols.extend(config.ALL_TARGET_FEATURES)
        
        # Also exclude any categorical columns that need encoding
        exclude_cols.extend(['season', 'pressure_tendency', 'precip_intensity'])
        
        # Get all numeric columns that aren't in exclude list
        feature_cols = [
            field.name for field in df.schema.fields
            if field.name not in exclude_cols
            and field.dataType.simpleString() in ['double', 'int', 'bigint', 'float']
        ]
        
        return feature_cols


if __name__ == "__main__":
    print("Feature Engineering Module")
    print("Use engineer_all_features() to create all time series features")
