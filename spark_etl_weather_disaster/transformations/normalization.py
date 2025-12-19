"""
Data Normalization Functions
Chuẩn hóa units, formats, timestamps cho 4 nguồn
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class DataNormalizer:
    
    @staticmethod
    def normalize_weather_data(df: DataFrame) -> DataFrame:
        """Normalize weather data units"""
        print("📏 Normalizing weather data...")
        
        # Convert Kelvin to Celsius and Fahrenheit
        df = df.withColumn("temp_celsius", col("temperature") - 273.15)
        df = df.withColumn("temp_fahrenheit", (col("temp_celsius") * 9/5) + 32)
        
        # Convert wind speed from m/s to km/h
        df = df.withColumn("wind_speed_kmh", col("wind_speed") * 3.6)
        
        # Rename for consistency
        df = df.withColumnRenamed("humidity", "humidity_pct")
        df = df.withColumnRenamed("pressure", "pressure_hpa")
        
        # Combine rain + snow = precipitation
        df = df.withColumn("precipitation_mm", col("rain_1h") + col("snow_1h"))
        
        # Classify weather severity
        df = df.withColumn(
            "weather_severity",
            when(col("precipitation_mm") > 20, "severe")
            .when(col("precipitation_mm") > 5, "moderate")
            .when(col("wind_speed") > 20, "moderate")
            .otherwise("normal")
        )
        
        # Standardize weather condition
        df = df.withColumn(
            "weather_condition",
            when(col("weather_description").contains("rain"), "rain")
            .when(col("weather_description").contains("snow"), "snow")
            .when(col("weather_description").contains("storm"), "storm")
           .when(col("weather_description").contains("cloud"), "cloudy")
            .otherwise("clear")
        )
        
        print("   ✅ Weather data normalized")
        return df
    
    @staticmethod
    def normalize_311_data(df: DataFrame) -> DataFrame:
        """Normalize 311 requests"""
        print("📏 Normalizing 311 data...")
        
        # Standardize borough names
        borough_mapping = {
            "MANHATTAN": "Manhattan",
            "BROOKLYN": "Brooklyn",
            "QUEENS": "Queens",
            "BRONX": "Bronx",
            "STATEN ISLAND": "Staten Island"
        }
        
        for old, new in borough_mapping.items():
            df = df.withColumn(
                "borough",
                when(col("borough") == old, new).otherwise(col("borough"))
            )
        
        # Calculate response time (hours)
        df = df.withColumn(
            "response_time_hours",
            (unix_timestamp(col("closed_date")) - unix_timestamp(col("created_date"))) / 3600
        )
        
        # Flag weather-related requests
        weather_keywords = ["tree", "flood", "sewer", "water", "street condition", "pothole"]
        weather_condition = col("complaint_type").rlike("|".join(weather_keywords))
        
        df = df.withColumn("is_weather_related", weather_condition.cast("boolean"))
        
        print("   ✅ 311 data normalized")
        return df
    
    @staticmethod
    def normalize_taxi_data(df: DataFrame) -> DataFrame:
        """Normalize taxi trip data"""
        print("📏 Normalizing taxi data...")
        
        # Calculate trip duration (minutes)
        df = df.withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("tpep_dropoff_datetime")) - 
             unix_timestamp(col("tpep_pickup_datetime"))) / 60
        )
        
        # Calculate average speed (km/h)
        df = df.withColumn(
            "avg_speed_kmh",
            (col("trip_distance") * 1.60934) / (col("trip_duration_minutes") / 60)
        )
        
        # Extract time components
        df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        df = df.withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
        df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        
        # Categorize trip distance
        df = df.withColumn(
            "trip_category",
            when(col("trip_distance") < 2, "short")
            .when(col("trip_distance") < 10, "medium")
            .otherwise("long")
        )
        
        print("   ✅ Taxi data normalized")
        return df
    
    @staticmethod
    def normalize_collision_data(df: DataFrame) -> DataFrame:
        """Normalize collision data"""
        print("📏 Normalizing collision data...")
        
        # Combine date and time into timestamp
        df = df.withColumn(
            "crash_datetime",
            to_timestamp(concat(col("crash_date"), lit(" "), col("crash_time")))
        )
        
        # Extract time components
        df = df.withColumn("crash_hour", hour(col("crash_datetime")))
        df = df.withColumn("crash_day_of_week", dayofweek(col("crash_datetime")))
        
        # Calculate total casualties
        df = df.withColumn(
            "total_injured",
            col("number_of_persons_injured")
        )
        
        df = df.withColumn(
            "total_killed",
            col("number_of_persons_killed")
        )
        
        df = df.withColumn(
            "total_casualties",
            col("total_injured") + col("total_killed")
        )
        
        # Classify severity
        df = df.withColumn(
            "severity",
            when(col("total_killed") > 0, "fatal")
            .when(col("total_injured") >= 3, "severe")
            .when(col("total_injured") > 0, "minor")
            .otherwise("property_damage")
        )
        
        # Flag weather-related collisions
        weather_factors = ["rain", "snow", "slippery", "ice", "fog", "wind"]
        weather_condition = col("contributing_factor_vehicle_1").rlike("|".join(weather_factors))
        
        df = df.withColumn("is_weather_related", weather_condition.cast("boolean"))
        
        # Standardize borough
        df = df.withColumn("borough", initcap(col("borough")))
        
        print("   ✅ Collision data normalized")
        return df
