"""
✅ VERIFIED Data Schemas
Based on actual data sources verification

Updated: 2025-11-28
"""

from pyspark.sql.types import *

# ========================================
# 1. WEATHER DATA SCHEMA - LONG FORMAT
# ========================================
# Kaggle data is wide format, needs to be melted to long format
# After melt: datetime | city | temperature | humidity | ...

weather_schema_long = StructType([
    StructField("datetime", TimestampType(), False),
    StructField("city", StringType(), False),
    StructField("temperature", DoubleType(), True),      # Kelvin
    StructField("humidity", DoubleType(), True),         # %
    StructField("pressure", DoubleType(), True),         # hPa
    StructField("wind_speed", DoubleType(), True),       # m/s
    StructField("wind_direction", DoubleType(), True),   # degrees
    StructField("weather_description", StringType(), True),
    StructField("rain_1h", DoubleType(), True),          # mm - precipitation last hour
    StructField("snow_1h", DoubleType(), True),          # mm - snow last hour
    StructField("clouds_all", IntegerType(), True),      # % - cloud coverage
])

# Alias for backward compatibility
weather_schema = weather_schema_long

# ========================================
# 2. NYC 311 SERVICE REQUESTS SCHEMA
# ========================================
# ✅ VERIFIED with NYC Open Data

service_311_schema = StructType([
    StructField("unique_key", StringType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("closed_date", TimestampType(), True),
    StructField("agency", StringType(), True),
    StructField("agency_name", StringType(), True),
    StructField("complaint_type", StringType(), True),
    StructField("descriptor", StringType(), True),
    StructField("location_type", StringType(), True),
    StructField("incident_zip", StringType(), True),
    StructField("incident_address", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("status", StringType(), True),
    # Optional additions:
    StructField("due_date", TimestampType(), True),
    StructField("resolution_description", StringType(), True),
    StructField("community_board", StringType(), True),
])

# ========================================
# 3. NYC TAXI TRIP SCHEMA - 2016
# ========================================
# ✅ VERIFIED - For 2016 data (has lat/lon coordinates)

taxi_trip_schema_2016 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), False),
    StructField("tpep_dropoff_datetime", TimestampType(), False),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),  # ✅ 2016 has this
    StructField("pickup_latitude", DoubleType(), True),   # ✅ 2016 has this
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),  # ✅ 2016 has this
    StructField("dropoff_latitude", DoubleType(), True),   # ✅ 2016 has this
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

# ========================================
# 4. NYC TAXI TRIP SCHEMA - 2017+
# ========================================
# ✅ VERIFIED - For 2017+ data (uses LocationID instead of lat/lon)

taxi_trip_schema_2017 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), False),
    StructField("tpep_dropoff_datetime", TimestampType(), False),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),  # ✅ 2017+ uses this
    StructField("DOLocationID", IntegerType(), True),  # ✅ 2017+ uses this
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),  # Added 2019
])

# Default to 2016 schema for backward compatibility
taxi_trip_schema = taxi_trip_schema_2016

# ========================================
# 5. MOTOR VEHICLE COLLISIONS SCHEMA
# ========================================
# ✅ VERIFIED with NYC Open Data

collision_schema = StructType([
    StructField("collision_id", StringType(), True),  # Unique ID
    StructField("crash_date", DateType(), False),
    StructField("crash_time", StringType(), False),
    StructField("borough", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("on_street_name", StringType(), True),
    StructField("cross_street_name", StringType(), True),
    StructField("off_street_name", StringType(), True),
    StructField("number_of_persons_injured", IntegerType(), True),
    StructField("number_of_persons_killed", IntegerType(), True),
    StructField("number_of_pedestrians_injured", IntegerType(), True),
    StructField("number_of_pedestrians_killed", IntegerType(), True),
    StructField("number_of_cyclist_injured", IntegerType(), True),
    StructField("number_of_cyclist_killed", IntegerType(), True),
    StructField("number_of_motorist_injured", IntegerType(), True),
    StructField("number_of_motorist_killed", IntegerType(), True),
    StructField("contributing_factor_vehicle_1", StringType(), True),
    StructField("contributing_factor_vehicle_2", StringType(), True),
    StructField("vehicle_type_code_1", StringType(), True),
    StructField("vehicle_type_code_2", StringType(), True),
])

# ========================================
# PROCESSED/ENRICHED SCHEMA (Output)
# ========================================

processed_integrated_schema = StructType([
    # Identifiers
    StructField("event_id", StringType(), False),
    StructField("event_datetime", TimestampType(), False),
    StructField("source_type", StringType(), False),
    
    # Location
    StructField("borough", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    
    # Weather features
    StructField("temperature_celsius", DoubleType(), True),
    StructField("temperature_fahrenheit", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("pressure_hpa", DoubleType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("weather_severity", StringType(), True),
    
    # Impact indicators
    StructField("service_requests_count", IntegerType(), True),
    StructField("taxi_trips_count", IntegerType(), True),
    StructField("collision_count", IntegerType(), True),
    StructField("total_injuries", IntegerType(), True),
    StructField("total_fatalities", IntegerType(), True),
    
    # Disaster risk
    StructField("disaster_risk_score", DoubleType(), True),
    StructField("traffic_impact_score", DoubleType(), True),
    StructField("emergency_level", StringType(), True),
    
    # Metadata
    StructField("processing_timestamp", TimestampType(), False),
    StructField("data_quality_score", DoubleType(), True),
])


# ========================================
# HELPER FUNCTIONS
# ========================================

def get_schema(source_type, year=2016):
    """
    Return schema based on source type and year
    
    Args:
        source_type: "weather", "311", "taxi", "collision", "processed"
        year: For taxi data (2016 or 2017+)
    
    Returns:
        StructType schema
    """
    schemas = {
        "weather": weather_schema_long,
        "311": service_311_schema,
        "taxi_2016": taxi_trip_schema_2016,
        "taxi_2017": taxi_trip_schema_2017,
        "collision": collision_schema,
        "processed": processed_integrated_schema
    }
    
    if source_type == "taxi":
        return taxi_trip_schema_2016 if year <= 2016 else taxi_trip_schema_2017
    
    return schemas.get(source_type, None)


def get_column_names(source_type, year=2016):
    """
    Get list of column names for a schema
    
    Args:
        source_type: Schema type
        year: Year for taxi schema
    
    Returns:
        List of column names
    """
    schema = get_schema(source_type, year)
    if schema:
        return [field.name for field in schema.fields]
    return []
