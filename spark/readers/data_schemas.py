"""
✅ WEATHER DATA SCHEMA
Schema định nghĩa cho dữ liệu thời tiết đọc từ Kafka/JSON
"""

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ========================================
# WEATHER SCHEMA
# Fields: datetime, City, temperature, humidity, pressure, weather_desc, wind_direction, wind_speed
# ========================================

weather_schema = StructType([
    # Dùng StringType cho datetime để tránh lỗi parse ngay từ đầu (sẽ cast sang Timestamp sau)
    StructField("datetime", StringType(), True),
    
    # Lưu ý: Kafka Producer của bạn đang gửi key là "City" (viết hoa chữ C)
    StructField("City", StringType(), True),
    
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("weather_desc", StringType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True)
])

# Alias để code cũ không bị lỗi nếu có gọi weather_schema_long
weather_schema_long = weather_schema