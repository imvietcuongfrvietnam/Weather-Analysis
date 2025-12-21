"""
Data Enrichment Functions
Thêm computed fields, risk scores (Chỉ dùng dữ liệu Weather)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class DataEnricher:
    
    # Bỏ __init__ chứa metadata join vì tạm thời không dùng
    
    def enrich_with_disaster_risk(self, df: DataFrame) -> DataFrame:
        """
        Tính điểm rủi ro thiên tai (0-100) dựa trên các chỉ số thời tiết hiện có.
        """
        print("✨ Calculating disaster risk scores...")
        
        # Khởi tạo điểm rủi ro mặc định là 0
        risk_score = lit(0)
        
        # 1. Rủi ro do Gió (Wind Risk)
        # Gió > 50km/h là nguy hiểm (+30 điểm)
        # Gió > 20km/h là đáng chú ý (+15 điểm)
        risk_score = risk_score + when(col("wind_speed") > 50, 30) \
                                  .when(col("wind_speed") > 20, 15) \
                                  .otherwise(0)
        
        # 2. Rủi ro Nhiệt độ (Extreme Temp)
        # Quá nóng (>40) hoặc quá lạnh (<0)
        risk_score = risk_score + when(col("temperature") < 0, 20) \
                                  .when(col("temperature") > 40, 20) \
                                  .when(col("temperature") > 35, 10) \
                                  .otherwise(0)
        
        # 3. Rủi ro Áp suất (Pressure)
        # Áp suất thấp (<990 hPa) thường báo hiệu bão
        risk_score = risk_score + when(col("pressure") < 990, 25) \
                                  .when(col("pressure") < 1000, 10) \
                                  .otherwise(0)
        
        # 4. Rủi ro dựa trên Mô tả thời tiết (Weather Description)
        # Nếu mô tả có chữ "storm", "rain", "snow"
        desc_lower = lower(col("weather_desc"))
        risk_score = risk_score + when(desc_lower.contains("storm"), 30) \
                                  .when(desc_lower.contains("rain"), 15) \
                                  .when(desc_lower.contains("snow"), 15) \
                                  .otherwise(0)
        
        df = df.withColumn("disaster_risk_score", risk_score)
        
        # Phân loại mức độ khẩn cấp (Labeling)
        df = df.withColumn(
            "emergency_level",
            when(col("disaster_risk_score") >= 60, "CRITICAL")
            .when(col("disaster_risk_score") >= 40, "HIGH")
            .when(col("disaster_risk_score") >= 20, "MEDIUM")
            .otherwise("LOW")
        )
        
        return df
    
    def add_ml_features(self, df: DataFrame) -> DataFrame:
        """
        Tạo các features đơn giản cho ML (Feature Engineering)
        """
        print("✨ Adding ML features...")
        
        # 1. Feature thời gian (Time-based features)
        # Trích xuất giờ (0-23) để biết ban ngày/ban đêm
        df = df.withColumn("hour", hour(col("datetime")))
        
        # Giả định ban ngày từ 6h sáng đến 6h tối
        df = df.withColumn("is_daytime", 
                          when((col("hour") >= 6) & (col("hour") <= 18), 1).otherwise(0))
        
        # Trích xuất tháng để biết mùa
        df = df.withColumn("month", month(col("datetime")))
        
        # 2. Chỉ số tiện nghi (Comfort Index)
        # Công thức tự chế đơn giản: Càng lệch khỏi 25 độ và 50% ẩm thì càng khó chịu
        temp_diff = abs(col("temperature") - 25)
        humid_diff = abs(col("humidity") - 50)
        
        # Index càng cao càng khó chịu
        df = df.withColumn(
            "discomfort_index",
            (temp_diff * 1.5) + (humid_diff * 0.5) + (col("wind_speed") * 0.2)
        )
        
        # 3. Timestamp xử lý (Metadata cho việc debug)
        df = df.withColumn("processing_timestamp", current_timestamp())
        
        return df