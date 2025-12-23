"""
Data Enrichment Functions
Thực hiện biến đổi dữ liệu nâng cao:
1. Stateless: Tính toán rủi ro, chỉ số khí tượng chuẩn (Heat Index, Wind Chill).
2. Stateful (Optional): Window Functions cho xu hướng (Trend).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

class DataEnricher:
    
    def enrich_with_disaster_risk(self, df: DataFrame) -> DataFrame:
        """
        Tính điểm rủi ro thiên tai (0-100) dựa trên logic nghiệp vụ phức tạp.
        """
        print("✨ Calculating disaster risk scores...")
        
        # --- 1. Chuẩn hóa dữ liệu đầu vào ---
        # Đảm bảo wind_speed không âm
        wind = when(col("wind_speed") < 0, 0).otherwise(col("wind_speed"))
        
        # --- 2. Tính điểm thành phần (Weighted Scoring) ---
        
        # Wind Score: Gió giật mạnh nguy hiểm theo cấp số nhân
        # Sử dụng hàm pow() để tăng trọng số cho giá trị cực đoan
        wind_score = when(wind > 100, 40) \
                    .when(wind > 50, 30) \
                    .when(wind > 20, 10 + (wind - 20) * 0.5) \
                    .otherwise(0)

        # Temp Score: Rủi ro nhiệt độ cực đoan
        temp_score = when(col("temperature") < -10, 30) \
                     .when(col("temperature") > 45, 30) \
                     .when((col("temperature") > 35) | (col("temperature") < 0), 15) \
                     .otherwise(0)
        
        # Pressure Score: Áp suất thấp là dấu hiệu bão (Cyclone)
        # 1013 hPa là chuẩn. Dưới 980 là rất nguy hiểm.
        pressure_score = when(col("pressure") < 980, 30) \
                         .when(col("pressure") < 1000, 15) \
                         .otherwise(0)

        # Keyword Score: Phân tích text mô tả
        desc_lower = lower(col("weather_desc"))
        keyword_score = when(desc_lower.contains("hurricane") | desc_lower.contains("tornado"), 50) \
                        .when(desc_lower.contains("storm") | desc_lower.contains("thunder"), 30) \
                        .when(desc_lower.contains("snow") | desc_lower.contains("rain"), 15) \
                        .otherwise(0)

        # --- 3. Tổng hợp và Cắt ngọn (Clipping) ---
        total_risk = wind_score + temp_score + pressure_score + keyword_score
        
        # Dùng hàm least để đảm bảo max là 100
        df = df.withColumn("disaster_risk_score", least(lit(100), total_risk))
        
        # Phân loại mức độ
        df = df.withColumn(
            "emergency_level",
            when(col("disaster_risk_score") >= 70, "CRITICAL")
            .when(col("disaster_risk_score") >= 40, "HIGH")
            .when(col("disaster_risk_score") >= 20, "MEDIUM")
            .otherwise("LOW")
        )
        
        return df
    
    def add_meteorological_indices(self, df: DataFrame) -> DataFrame:
        """
        Thêm các chỉ số khí tượng CHUẨN KHOA HỌC (Heat Index, Wind Chill).
        Đây là phần 'Advanced' về mặt logic toán học.
        """
        print("✨ Adding scientific meteorological indices...")

        T = col("temperature")
        R = col("humidity")
        V = col("wind_speed")

        # 1. Heat Index (Chỉ số nóng bức - Feels Like) - Công thức NOAA
        # Chỉ tính khi nhiệt độ >= 27C (80F). Ở đây dùng công thức đơn giản hóa.
        # HI = c1 + c2T + c3R + c4TR + c5T^2 + ... (Rất dài, dùng bản rút gọn xấp xỉ)
        # Công thức đơn giản: T + 0.55 * (1 - 0.01*R) * (T - 14.5) (Lanzhou University)
        heat_index = T + 0.55 * (1 - (0.01 * R)) * (T - 14.5)
        
        # Chỉ áp dụng Heat Index khi trời nóng
        df = df.withColumn("heat_index_c", 
                           when(T >= 25, heat_index).otherwise(T))

        # 2. Wind Chill (Độ lạnh của gió) - Công thức chuẩn Bắc Mỹ
        # Twc = 13.12 + 0.6215Ta - 11.37v^0.16 + 0.3965Ta*v^0.16
        # Chỉ tính khi T <= 10 độ C và V > 4.8 km/h
        wind_chill = 13.12 + (0.6215 * T) - (11.37 * pow(V, 0.16)) + (0.3965 * T * pow(V, 0.16))
        
        df = df.withColumn("wind_chill_c", 
                           when((T <= 10) & (V > 5), wind_chill).otherwise(T))

        # 3. Time features
        df = df.withColumn("hour", hour(col("datetime")))
        df = df.withColumn("is_daytime", when((col("hour") >= 6) & (col("hour") <= 18), 1).otherwise(0))
        
        return df

    def add_advanced_window_features(self, df: DataFrame) -> DataFrame:
        """
        ⚠️ CHỈ DÙNG CHO BATCH/ML PIPELINE (Không dùng cho Streaming ETL cơ bản)
        Sử dụng Window Functions để tính xu hướng.
        """
        print("✨ Adding time-series window features...")
        
        # Partition theo City, sắp xếp theo thời gian
        # Window này giúp so sánh dữ liệu của chính thành phố đó với quá khứ
        window_spec = Window.partitionBy("city").orderBy("datetime")
        
        # 1. Lag Features: Nhiệt độ 1 giờ trước
        df = df.withColumn("temp_lag_1", lag("temperature", 1).over(window_spec))
        
        # 2. Delta Features: Sự thay đổi nhiệt độ (Temp Trend)
        df = df.withColumn("temp_change", col("temperature") - col("temp_lag_1"))
        
        # 3. Rolling Average: Áp suất trung bình 3 bản ghi gần nhất (Làm mượt dữ liệu)
        # rowsBetween(-2, 0) nghĩa là: dòng hiện tại và 2 dòng trước đó
        window_rolling = window_spec.rowsBetween(-2, 0)
        df = df.withColumn("pressure_rolling_avg", avg("pressure").over(window_rolling))
        
        return df