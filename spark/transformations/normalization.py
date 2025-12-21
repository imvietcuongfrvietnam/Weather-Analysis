"""
Data Normalization Functions
Chuẩn hóa dữ liệu Weather để thống nhất định dạng
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, lower, trim

class DataNormalizer:
    
    @staticmethod
    def normalize_weather_data(df: DataFrame) -> DataFrame:
        """
        Chuẩn hóa các trường dữ liệu thời tiết
        """
        print("⚖️  Normalizing weather data...")
        
        # 1. Làm tròn số liệu (Dashboard hiển thị cho đẹp)
        # Nhiệt độ, tốc độ gió, áp suất -> lấy 2 chữ số thập phân
        cols_to_round = ["temperature", "wind_speed", "wind_direction", "humidity", "pressure"]
        
        for col_name in cols_to_round:
            if col_name in df.columns:
                df = df.withColumn(col_name, round(col(col_name), 2))
        
        # 2. Chuẩn hóa văn bản (Text Normalization)
        # Đưa về chữ thường để dễ query/filter sau này (VD: "Rain" -> "rain")
        if "weather_desc" in df.columns:
            df = df.withColumn("weather_desc", lower(trim(col("weather_desc"))))
            
        if "city" in df.columns:
            # Tên thành phố thì nên Viết Hoa Chữ Cái Đầu (Title Case) cho đẹp
            from pyspark.sql.functions import initcap
            df = df.withColumn("city", initcap(trim(col("city"))))
            
        return df