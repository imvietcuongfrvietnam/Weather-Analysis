"""
Data Enrichment Functions
Thêm computed fields, ML features, risk scores
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

class DataEnricher:
    
    @staticmethod
    def enrich_with_disaster_risk(df: DataFrame) -> DataFrame:
        """
        Calculate disaster risk score based on weather conditions
        Score: 0-100 (higher = more危险)
        """
        print("✨ Calculating disaster risk scores...")
        
        # Factors:
        # - High precipitation (+30)
        # - High wind (+20)
        # - Extreme temperature (+15)
        # - Low pressure (+15)
        # - Severe weather condition (+20)
        
        risk_score = lit(0)
        
        # Precipitation risk
        risk_score = risk_score + when(col("precipitation_mm") > 20, 30) \
                                  .when(col("precipitation_mm") > 10, 20) \
                                  .when(col("precipitation_mm") > 5, 10) \
                                  .otherwise(0)
        
        # Wind risk
        risk_score = risk_score + when(col("wind_speed_kmh") > 50, 20) \
                                  .when(col("wind_speed_kmh") > 30, 10) \
                                  .otherwise(0)
        
        # Temperature extremes
        risk_score = risk_score + when(col("temp_celsius") < -10, 15) \
                                  .when(col("temp_celsius") > 35, 15) \
                                  .when(col("temp_celsius") < 0, 5) \
                                  .when(col("temp_celsius") > 30, 5) \
                                  .otherwise(0)
        
        # Pressure (low pressure = storms)
        risk_score = risk_score + when(col("pressure_hpa") < 1000, 15) \
                                  .when(col("pressure_hpa") < 1010, 5) \
                                  .otherwise(0)
        
        # Weather condition
        risk_score = risk_score + when(col("weather_condition") == "storm", 20) \
                                  .when(col("weather_condition") == "snow", 15) \
                                  .when(col("weather_condition") == "rain", 10) \
                                  .otherwise(0)
        
        df = df.withColumn("disaster_risk_score", risk_score)
        
        # Emergency level
        df = df.withColumn(
            "emergency_level",
            when(col("disaster_risk_score") >= 70, "critical")
            .when(col("disaster_risk_score") >= 50, "high")
            .when(col("disaster_risk_score") >= 30, "medium")
            .otherwise("low")
        )
        
        print("   ✅ Disaster risk scores calculated")
        return df
    
    @staticmethod
    def enrich_with_traffic_impact(spark: SparkSession, 
                                    weather_df: DataFrame,
                                    taxi_df: DataFrame,
                                    collision_df: DataFrame) -> DataFrame:
        """
        Join weather with taxi/collision data to calculate traffic impact
        """
        print("✨ Calculating traffic impact scores...")
        
        # Aggregate taxi trips by hour
        taxi_agg = taxi_df.groupBy(
            to_date(col("tpep_pickup_datetime")).alias("date"),
            hour(col("tpep_pickup_datetime")).alias("hour")
        ).agg(
            count("*").alias("trip_count"),
            avg("trip_duration_minutes").alias("avg_duration"),
            avg("avg_speed_kmh").alias("avg_speed")
        )
        
        # Aggregate collisions by hour
        collision_agg = collision_df.groupBy(
            col("crash_date").alias("date"),
            col("crash_hour").alias("hour")
        ).agg(
            count("*").alias("collision_count"),
            sum("total_injured").alias("total_injured"),
            sum("total_killed").alias("total_killed")
        )
        
        # Weather with date/hour
        weather_hourly = weather_df.withColumn("date", to_date(col("datetime"))) \
                                    .withColumn("hour", hour(col("datetime")))
        
        # Join all
        enriched = weather_hourly \
            .join(taxi_agg, on=["date", "hour"], how="left") \
            .join(collision_agg, on=["date", "hour"], how="left")
        
        # Fill nulls
        enriched = enriched.fillna({
            "trip_count": 0,
            "collision_count": 0,
            "total_injured": 0,
            "total_killed": 0
        })
        
        # Calculate traffic impact score (0-100)
        # Normal baseline: 1000 trips/hour, 2 collisions/hour
        # Lower trips + higher collisions = higher impact
        
        impact_score = lit(0)
        
        # Trip reduction impact
        impact_score = impact_score + when(col("trip_count") < 500, 30) \
                                      .when(col("trip_count") < 800, 15) \
                                      .otherwise(0)
        
        # Collision increase impact
        impact_score = impact_score + when(col("collision_count") >= 10, 40) \
                                      .when(col("collision_count") >= 5, 20) \
                                      .when(col("collision_count") >= 3, 10) \
                                      .otherwise(0)
        
        # Casualties impact
        impact_score = impact_score + when(col("total_killed") > 0, 30) \
                                      .when(col("total_injured") >= 5, 20) \
                                      .when(col("total_injured") > 0, 10) \
                                      .otherwise(0)
        
        enriched = enriched.withColumn("traffic_impact_score", impact_score)
        
        print("   ✅ Traffic impact scores calculated")
        return enriched
    
    @staticmethod
    def add_ml_features(df: DataFrame) -> DataFrame:
        """
        Add additional features for ML models
        """
        print("✨ Adding ML features...")
        
        # Time-based features
        df = df.withColumn("is_weekend", when(dayofweek(col("datetime")).isin([1, 7]), 1).otherwise(0))
        df = df.withColumn("is_rush_hour", 
                          when(((hour(col("datetime")) >= 7) & (hour(col("datetime")) <= 9)) |
                               ((hour(col("datetime")) >= 17) & (hour(col("datetime")) <= 19)), 1).otherwise(0)
                          )
        
        df = df.withColumn("month", month(col("datetime")))
        df = df.withColumn("season",
                          when(col("month").isin([12, 1, 2]), "winter")
                          .when(col("month").isin([3, 4, 5]), "spring")
                          .when(col("month").isin([6, 7, 8]), "summer")
                          .otherwise("fall"))
        
        # Weather index (composite score)
        df = df.withColumn(
            "weather_comfort_index",
            100 - (
                abs(col("temp_celsius") - 20) * 2 +  # Ideal: 20°C
                abs(col("humidity_pct") - 50) * 0.5 +  # Ideal: 50%
                col("wind_speed_kmh") * 0.5 +
                col("precipitation_mm") * 5
            )
        )
        
        # Rolling averages (for time series)
        window_spec = Window.orderBy("datetime").rowsBetween(-23, 0)  # 24 hours
        
        df = df.withColumn("temp_24h_avg", avg("temp_celsius").over(window_spec))
        df = df.withColumn("precip_24h_sum", sum("precipitation_mm").over(window_spec))
        
        print("   ✅ ML features added")
        return df
    
    @staticmethod
    def add_processing_metadata(df: DataFrame) -> DataFrame:
        """Add processing metadata"""
        df = df.withColumn("processing_timestamp", current_timestamp())
        df = df.withColumn("processing_date", current_date())
        
        # Data quality score (simplified)
        # Based on completeness of key fields
        quality_score = lit(100)
        
        # Deduct points for missing data
        for col_name in ["temperature", "humidity", "pressure"]:
            if col_name in df.columns:
                quality_score = quality_score - when(col(col_name).isNull(), 10).otherwise(0)
        
        df = df.withColumn("data_quality_score", quality_score)
        
        return df
