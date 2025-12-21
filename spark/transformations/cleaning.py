"""
Data Cleaning Functions
Chuyên biệt cho dữ liệu thời tiết (Weather Data)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, trim, initcap, when

class DataCleaner:
    
    @staticmethod
    def clean_weather_data(df: DataFrame) -> DataFrame:
        """
        Làm sạch dữ liệu thời tiết
        Input columns: datetime, City, temperature, humidity, pressure, weather_desc, wind_direction, wind_speed
        """
        print("🧹 Cleaning weather data...")
        
        # 1. Đổi tên cột cho chuẩn (City -> city)
        if "City" in df.columns:
            df = df.withColumnRenamed("City", "city")
            
        # 2. Chuyển đổi Datetime (Quan trọng nhất)
        # Input đang là String (do Schema định nghĩa), cần chuyển sang Timestamp để tính toán
        df = df.withColumn("datetime", to_timestamp(col("datetime")))
        
        # 3. Ép kiểu số (Double) cho các cột chỉ số
        numeric_cols = ["temperature", "humidity", "pressure", "wind_speed", "wind_direction"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast("double"))
        
        # 4. Xóa dữ liệu rác (Null) ở các trường quan trọng
        # Nếu không có thời gian, thành phố hoặc nhiệt độ -> Dòng này vô nghĩa
        df = df.dropna(subset=["datetime", "city", "temperature"])
        
        # 5. Xử lý chuỗi (String Cleaning)
        # weather_desc: bỏ khoảng trắng thừa, viết hoa chữ cái đầu (VD: " clear sky " -> "Clear Sky")
        if "weather_desc" in df.columns:
            df = df.withColumn("weather_desc", initcap(trim(col("weather_desc"))))
            
        if "city" in df.columns:
            df = df.withColumn("city", trim(col("city")))

        # 6. Validate dữ liệu (Business Logic)
        
        # Nhiệt độ (C): Chặn các giá trị vô lý (Ví dụ: -100 độ hoặc 100 độ)
        # Giả định dữ liệu đầu vào là Celsius (-50 đến 60 độ là ngưỡng an toàn trên trái đất)
        df = df.filter((col("temperature") >= -50) & (col("temperature") <= 60))
        
        # Độ ẩm (%): Phải từ 0 đến 100
        df = df.filter((col("humidity") >= 0) & (col("humidity") <= 100))
        
        # Áp suất (hPa): Thường từ 870 đến 1085 (kỷ lục thế giới). Lấy rộng ra 800-1200.
        df = df.filter((col("pressure") >= 800) & (col("pressure") <= 1200))
        
        # Tốc độ gió: Không thể âm
        df = df.filter(col("wind_speed") >= 0)
        
        # Hướng gió: 0 - 360 độ
        df = df.filter((col("wind_direction") >= 0) & (col("wind_direction") <= 360))

        # 7. Xử lý trùng lặp (Deduplication)
        # Nếu 1 thành phố có 2 bản tin trong cùng 1 thời điểm -> Giữ 1 cái
        # Lưu ý: Trong Streaming, dropDuplicates cần watermark, nhưng ở đây ta làm clean mức row cơ bản
        df = df.dropDuplicates(["datetime", "city"])
        
        # Log debug số lượng (Lưu ý: count() trong streaming có thể gây chậm, chỉ dùng khi debug)
        # print(f"   ✅ Weather data cleaned.") 
        
        return df

    @staticmethod
    def remove_outliers_iqr(df: DataFrame, column: str) -> DataFrame:
        """
        Loại bỏ ngoại lai bằng phương pháp IQR (Dùng cho Batch Job/ML)
        Không khuyến nghị dùng cho Streaming vì cần tính toán toàn cục
        """
        try:
            quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            return df.filter(
                (col(column) >= lower_bound) & (col(column) <= upper_bound)
            )
        except Exception as e:
            print(f"⚠️ Không thể lọc Outlier cho cột {column}: {e}")
            return df