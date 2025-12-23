"""
Data Loader - Load weather data from MinIO
Đọc dữ liệu thời tiết đã được làm sạch và chuẩn hóa (Output của main_etl.py)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, to_timestamp, min as spark_min, max as spark_max, avg, stddev
from pyspark.sql.types import TimestampType
import sys
import os

# Thêm đường dẫn để import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import config
except ImportError:
    # Fallback Config nếu không tìm thấy file config.py
    class Config:
        MINIO_BUCKET = "weather-data"
        # Đường dẫn tới dữ liệu đầu ra của quá trình Normalization
        MINIO_INPUT_PATH = f"s3a://{MINIO_BUCKET}/enriched/weather" 
        
        # Các cột mục tiêu quan trọng (Cập nhật mới nhất)
        ALL_TARGET_FEATURES = [
            "temperature", 
            "humidity", 
            "pressure", 
            "wind_speed", 
            "wind_direction"
        ]
        
        # Các cột số liên tục
        CONTINUOUS_FEATURES = ALL_TARGET_FEATURES
        
        # Ngưỡng kiểm tra chất lượng dữ liệu
        MAX_MISSING_PCT = 0.05       # 5%
        MIN_DAYS_HISTORY = 1         # Tối thiểu 1 ngày
        MIN_TRAINING_RECORDS = 50    # Tối thiểu 50 dòng
        
    config = Config()

class WeatherDataLoader:
    """Load and validate weather data from MinIO"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Mặc định load từ folder enriched/weather (Nơi MinIOWriter ghi Parquet)
        if hasattr(config, 'MINIO_INPUT_PATH'):
            self.input_path = config.MINIO_INPUT_PATH
        else:
            self.input_path = "s3a://weather-data/enriched/weather"
        
    def load_data(self, city: str = None, limit_rows: int = None) -> DataFrame:
        """
        Load weather data (Parquet) from MinIO
        """
        print(f"\n📂 Loading data from: {self.input_path}")
        
        try:
            # 1. Đọc tất cả file Parquet từ MinIO
            # mergeSchema=True giúp đọc được nhiều file dù schema có thay đổi nhỏ
            df = self.spark.read.option("mergeSchema", "true").parquet(self.input_path)
            
            total_count = df.count()
            print(f"   ✅ Successfully loaded {total_count} records")
            
            if total_count == 0:
                print("   ⚠️  Warning: Dataset is empty!")
                return df
            
            # 2. Lọc theo thành phố (nếu có yêu cầu)
            if city:
                df = df.filter(col("city") == city)
                print(f"   🏙️  Filtered to city: {city} ({df.count()} records)")
            
            # 3. Chuẩn hóa cột Datetime
            if "datetime" not in df.columns:
                print("   ⚠️  Column 'datetime' not found! Trying to find alternative...")
                raise ValueError("Column 'datetime' not found in data!")
            
            # Ép kiểu sang Timestamp nếu nó đang là String
            if not isinstance(df.schema["datetime"].dataType, TimestampType):
                df = df.withColumn("datetime", to_timestamp(col("datetime")))
            
            # 4. Sắp xếp theo thời gian (Quan trọng cho Time Series)
            df = df.orderBy("datetime")
            
            # 5. Giới hạn số dòng (cho mục đích test nhanh)
            if limit_rows:
                df = df.limit(limit_rows)
                print(f"   ⚠️  Limited to {limit_rows} records for testing")
            
            return df
            
        except Exception as e:
            print(f"   ❌ Error loading data: {e}")
            # Trả về DataFrame rỗng thay vì crash chương trình
            from pyspark.sql.types import StructType
            return self.spark.createDataFrame([], StructType([]))
    
    def validate_data(self, df: DataFrame) -> dict:
        """
        Kiểm tra chất lượng dữ liệu (Data Quality Check)
        """
        print("\n🔍 Validating data quality...")
        
        if df.rdd.isEmpty():
            print("   ❌ Validation Failed: DataFrame is empty")
            return {'quality_score': 0}

        total_records = df.count()
        validation_results = {
            'total_records': total_records,
            'missing_values': {},
            'data_range': {},
            'quality_score': 100.0
        }
        
        # 1. Kiểm tra các cột bắt buộc (Features)
        required_features = config.ALL_TARGET_FEATURES
        missing_features = [f for f in required_features if f not in df.columns]
        
        if missing_features:
            print(f"   ⚠️  Missing required features: {missing_features}")
            validation_results['missing_features'] = missing_features
            validation_results['quality_score'] -= 20
        
        # 2. Kiểm tra giá trị thiếu (Missing Values)
        print("   📊 Checking missing values...")
        features_to_check = [f for f in config.ALL_TARGET_FEATURES if f in df.columns]
        
        for feature in features_to_check:
            null_count = df.filter(col(feature).isNull() | isnan(col(feature))).count()
            null_pct = (null_count / total_records) * 100
            
            validation_results['missing_values'][feature] = {
                'count': null_count,
                'percentage': null_pct
            }
            
            if null_pct > config.MAX_MISSING_PCT * 100:
                print(f"      ⚠️  {feature}: {null_pct:.2f}% missing (High!)")
                validation_results['quality_score'] -= 5
        
        # 3. Kiểm tra dải dữ liệu (Data Range) - Phát hiện Outliers
        print("   📈 Checking data ranges...")
        # Lọc các cột số thực tế có trong DF
        numeric_cols = [f for f in config.CONTINUOUS_FEATURES if f in df.columns]
        
        if numeric_cols:
            # Tính min/max một lần cho nhanh
            aggregations = []
            for col_name in numeric_cols:
                aggregations.append(spark_min(col_name).alias(f"min_{col_name}"))
                aggregations.append(spark_max(col_name).alias(f"max_{col_name}"))
            
            stats = df.agg(*aggregations).collect()[0]
            
            for feature in numeric_cols:
                min_val = stats[f"min_{feature}"]
                max_val = stats[f"max_{feature}"]
                
                validation_results['data_range'][feature] = {'min': min_val, 'max': max_val}
                print(f"      {feature}: [{min_val:.2f}, {max_val:.2f}]")
        
        # 4. Kiểm tra khoảng thời gian (Time Span)
        time_stats = df.agg(spark_min('datetime'), spark_max('datetime')).collect()[0]
        start_time = time_stats[0]
        end_time = time_stats[1]
        
        if start_time and end_time:
            time_span_days = (end_time - start_time).days
            validation_results['time_span_days'] = time_span_days
            print(f"\n   📅 Time span: {time_span_days} days ({start_time} to {end_time})")
            
            if time_span_days < config.MIN_DAYS_HISTORY:
                print(f"   ⚠️  Warning: Only {time_span_days} days of data (Need more history for ML)")
                validation_results['quality_score'] -= 15
        
        # 5. Kiểm tra số lượng bản ghi tối thiểu
        if total_records < config.MIN_TRAINING_RECORDS:
            print(f"   ❌ Insufficient data: {total_records} records (minimum: {config.MIN_TRAINING_RECORDS})")
            validation_results['quality_score'] -= 30
        
        # Kết luận
        quality_score = max(0, validation_results['quality_score']) # Không âm
        status = "GOOD" if quality_score >= 80 else ("ACCEPTABLE" if quality_score >= 60 else "POOR")
        print(f"\n   ✅ Data Quality Score: {quality_score:.1f}% - {status}")
        
        return validation_results
    
    def get_cities(self, df: DataFrame) -> list:
        """Lấy danh sách thành phố có trong dữ liệu"""
        if 'city' in df.columns:
            return [row.city for row in df.select('city').distinct().collect()]
        return []
    
    def summary_stats(self, df: DataFrame):
        """In thống kê tóm tắt"""
        print("\n" + "="*60)
        print("📊 DATA SUMMARY")
        print("="*60)
        
        if df.rdd.isEmpty():
            print("   (Empty DataFrame)")
            return

        print(f"Total Records:     {df.count()}")
        print(f"Total Columns:     {len(df.columns)}")
        
        cities = self.get_cities(df)
        print(f"Cities ({len(cities)}):      {', '.join(cities[:5])}...")
        
        print("\n📈 Sample Data (Top 5):")
        # Chọn các cột quan trọng để hiển thị
        display_cols = ['datetime', 'city'] + [c for c in config.ALL_TARGET_FEATURES if c in df.columns]
        df.select(display_cols).show(5, truncate=False)
        
        print("="*60 + "\n")

if __name__ == "__main__":
    print("Testing data loader module...")