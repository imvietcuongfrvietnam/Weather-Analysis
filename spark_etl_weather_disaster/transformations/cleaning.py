"""
Data Cleaning Functions
Clean data từ 4 nguồn: weather, 311, taxi, collisions
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class DataCleaner:
    
    @staticmethod
    def clean_weather_data(df: DataFrame) -> DataFrame:
        """Clean weather data"""
        print("🧹 Cleaning weather data...")
        
        # Remove nulls in critical columns
        df = df.dropna(subset=["datetime", "temperature"])
        
        # Remove duplicates
        df = df.dropDuplicates(["datetime", "city"])
        
        # Validate temperature range (Kelvin: 200-350)
        df = df.filter((col("temperature") >= 200) & (col("temperature") <= 350))
        
        # Validate humidity (0-100%)
        df = df.filter((col("humidity") >= 0) & (col("humidity") <= 100))
        
        # Validate pressure (900-1100 hPa)
        df = df.filter((col("pressure") >= 900) & (col("pressure") <= 1100))
        
        # Fill missing precipitation with 0
        df = df.fillna({"rain_1h": 0.0, "snow_1h": 0.0})
        
        print(f"   ✅ Weather data cleaned: {df.count()} records")
        return df
    
    @staticmethod
    def clean_311_data(df: DataFrame) -> DataFrame:
        """Clean 311 service requests"""
        print("🧹 Cleaning 311 data...")
        
        # Remove nulls
        df = df.dropna(subset=["unique_key", "created_date"])
        
        # Remove duplicates
        df = df.dropDuplicates(["unique_key"])
        
        # Clean text fields
        df = df.withColumn("complaint_type", trim(col("complaint_type")))
        df = df.withColumn("borough", upper(trim(col("borough"))))
        
        # Validate coordinates
        df = df.filter(
            (col("latitude").between(40.4, 41.0)) &
            (col("longitude").between(-74.3, -73.7))
        )
        
        print(f"   ✅ 311 data cleaned: {df.count()} records")
        return df
    
    @staticmethod
    def clean_taxi_data(df: DataFrame) -> DataFrame:
        """Clean taxi trip data"""
        print("🧹 Cleaning taxi data...")
        
        # Remove nulls
        df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        
        # Remove invalid trips (negative duration or distance)
        df = df.filter(col("trip_distance") > 0)
        df = df.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
        
        # Remove outliers in fare
        df = df.filter((col("fare_amount") > 0) & (col("fare_amount") < 500))
        
        # Validate passenger count
        df = df.filter((col("passenger_count") >= 1) & (col("passenger_count") <= 6))
        
        # Validate coordinates
        df = df.filter(
            (col("pickup_latitude").between(40.4, 41.0)) &
            (col("pickup_longitude").between(-74.3, -73.7))
        )
        
        print(f"   ✅ Taxi data cleaned: {df.count()} records")
        return df
    
    @staticmethod
    def clean_collision_data(df: DataFrame) -> DataFrame:
        """Clean collision data"""
        print("🧹 Cleaning collision data...")
        
        # Remove nulls
        df = df.dropna(subset=["crash_date", "crash_time"])
        
        # Clean borough names
        df = df.withColumn("borough", upper(trim(col("borough"))))
        
        # Ensure injury/death counts are non-negative
        injury_cols = [
            "number_of_persons_injured", "number_of_persons_killed",
            "number_of_pedestrians_injured", "number_of_cyclist_injured",
            "number_of_motorist_injured"
        ]
        
        for col_name in injury_cols:
            df = df.withColumn(col_name, when(col(col_name) < 0, 0).otherwise(col(col_name)))
        
        # Validate coordinates
        df = df.filter(
            (col("latitude").between(40.4, 41.0)) &
            (col("longitude").between(-74.3, -73.7))
        )
        
        print(f"   ✅ Collision data cleaned: {df.count()} records")
        return df
    
    @staticmethod
    def remove_outliers_iqr(df: DataFrame, column: str) -> DataFrame:
        """Remove outliers using IQR method"""
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        return df.filter(
            (col(column) >= lower_bound) & (col(column) <= upper_bound)
        )
