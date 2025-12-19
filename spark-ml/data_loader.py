"""
Data Loader - Load weather data from MinIO
Đọc dữ liệu thời tiết đã được làm giàu từ MinIO
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, to_timestamp
from pyspark.sql.types import TimestampType
import config

class WeatherDataLoader:
    """Load and validate weather data from MinIO"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.input_path = config.MINIO_INPUT_PATH
        
    def load_data(self, city: str = None, limit_rows: int = None) -> DataFrame:
        """
        Load enriched weather data from MinIO
        
        Args:
            city: Filter by specific city (optional)
            limit_rows: Limit number of rows for testing (optional)
            
        Returns:
            DataFrame with weather data sorted by datetime
        """
        print(f"\n📂 Loading data from: {self.input_path}")
        
        try:
            # Read all parquet files from MinIO
            df = self.spark.read.parquet(self.input_path)
            
            print(f"   ✅ Successfully loaded {df.count()} records")
            
            # Filter by city if specified
            if city:
                df = df.filter(col("city") == city)
                print(f"   🏙️  Filtered to city: {city} ({df.count()} records)")
            
            # Ensure datetime column exists and is properly typed
            if "datetime" not in df.columns:
                raise ValueError("Column 'datetime' not found in data!")
            
            # Cast to timestamp if needed
            if not isinstance(df.schema["datetime"].dataType, TimestampType):
                df = df.withColumn("datetime", to_timestamp(col("datetime")))
            
            # Sort by datetime for time series analysis
            df = df.orderBy("datetime")
            
            # Limit rows if specified (for testing)
            if limit_rows:
                df = df.limit(limit_rows)
                print(f"   ⚠️  Limited to {limit_rows} records for testing")
            
            return df
            
        except Exception as e:
            print(f"   ❌ Error loading data: {e}")
            raise
    
    def validate_data(self, df: DataFrame) -> dict:
        """
        Validate data quality and completeness
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict with validation results
        """
        print("\n🔍 Validating data quality...")
        
        total_records = df.count()
        validation_results = {
            'total_records': total_records,
            'missing_values': {},
            'data_range': {},
            'quality_score': 100.0
        }
        
        # Check for required target features
        required_features = config.ALL_TARGET_FEATURES
        missing_features = [f for f in required_features if f not in df.columns]
        
        if missing_features:
            print(f"   ⚠️  Missing required features: {missing_features}")
            validation_results['missing_features'] = missing_features
            validation_results['quality_score'] -= 20
        
        # Check missing values for each target feature
        print("   📊 Checking missing values...")
        for feature in config.ALL_TARGET_FEATURES:
            if feature in df.columns:
                null_count = df.filter(
                    col(feature).isNull() | isnan(col(feature))
                ).count()
                
                null_pct = (null_count / total_records) * 100 if total_records > 0 else 0
                validation_results['missing_values'][feature] = {
                    'count': null_count,
                    'percentage': null_pct
                }
                
                if null_pct > config.MAX_MISSING_PCT * 100:
                    print(f"      ⚠️  {feature}: {null_pct:.2f}% missing (threshold: {config.MAX_MISSING_PCT*100}%)")
                    validation_results['quality_score'] -= 5
                else:
                    print(f"      ✅ {feature}: {null_pct:.2f}% missing")
        
        # Check data range for continuous features
        print("   📈 Checking data ranges...")
        for feature in config.CONTINUOUS_FEATURES:
            if feature in df.columns:
                stats = df.select(
                    col(feature).alias("value")
                ).filter(
                    col(feature).isNotNull()
                ).agg({
                    'value': 'min',
                    'value': 'max',
                    'value': 'mean',
                    'value': 'stddev'
                }).collect()[0]
                
                validation_results['data_range'][feature] = {
                    'min': stats['min(value)'],
                    'max': stats['max(value)'],
                }
                
                print(f"      {feature}: [{stats['min(value)']:.2f}, {stats['max(value)']:.2f}]")
        
        # Check time span
        time_stats = df.agg({
            'datetime': 'min',
            'datetime': 'max'
        }).collect()[0]
        
        start_time = time_stats['min(datetime)']
        end_time = time_stats['max(datetime)']
        time_span_days = (end_time - start_time).days if start_time and end_time else 0
        
        validation_results['time_span_days'] = time_span_days
        validation_results['start_time'] = start_time
        validation_results['end_time'] = end_time
        
        print(f"\n   📅 Time span: {time_span_days} days ({start_time} to {end_time})")
        
        if time_span_days < config.MIN_DAYS_HISTORY:
            print(f"   ⚠️  Warning: Only {time_span_days} days of data (recommended: {config.MIN_DAYS_HISTORY}+ days)")
            validation_results['quality_score'] -= 15
        
        # Check minimum records
        if total_records < config.MIN_TRAINING_RECORDS:
            print(f"   ❌ Insufficient data: {total_records} records (minimum: {config.MIN_TRAINING_RECORDS})")
            validation_results['quality_score'] -= 30
        
        # Final quality assessment
        quality_score = validation_results['quality_score']
        if quality_score >= 80:
            print(f"\n   ✅ Data Quality Score: {quality_score:.1f}% - GOOD")
        elif quality_score >= 60:
            print(f"\n   ⚠️  Data Quality Score: {quality_score:.1f}% - ACCEPTABLE")
        else:
            print(f"\n   ❌ Data Quality Score: {quality_score:.1f}% - POOR")
        
        return validation_results
    
    def get_cities(self, df: DataFrame) -> list:
        """Get list of unique cities in the data"""
        if 'city' in df.columns:
            cities = df.select('city').distinct().rdd.map(lambda r: r[0]).collect()
            return cities
        return []
    
    def summary_stats(self, df: DataFrame):
        """Print summary statistics of the data"""
        print("\n" + "="*80)
        print("📊 DATA SUMMARY")
        print("="*80)
        
        # Basic info
        print(f"Total Records:     {df.count()}")
        print(f"Total Columns:     {len(df.columns)}")
        
        # Cities
        cities = self.get_cities(df)
        if cities:
            print(f"Cities:            {', '.join(cities)}")
        
        # Time range
        time_stats = df.agg({
            'datetime': 'min',
            'datetime': 'max'
        }).collect()[0]
        
        if time_stats['min(datetime)'] and time_stats['max(datetime)']:
            time_span = (time_stats['max(datetime)'] - time_stats['min(datetime)']).days
            print(f"Time Range:        {time_stats['min(datetime)']} to {time_stats['max(datetime)']}")
            print(f"Time Span:         {time_span} days")
        
        # Feature statistics
        print("\n📈 Feature Statistics:")
        for feature in config.ALL_TARGET_FEATURES:
            if feature in df.columns:
                if feature in config.CONTINUOUS_FEATURES:
                    stats = df.select(feature).filter(
                        col(feature).isNotNull()
                    ).agg({
                        feature: 'min',
                        feature: 'max',
                        feature: 'mean',
                        feature: 'stddev'
                    }).collect()[0]
                    
                    print(f"  {feature:20s}: min={stats[f'min({feature})']:8.2f}, "
                          f"max={stats[f'max({feature})']:8.2f}, "
                          f"mean={stats[f'avg({feature})']:8.2f}, "
                          f"std={stats[f'stddev_samp({feature})']:8.2f}")
                else:
                    # Categorical feature
                    distinct_count = df.select(feature).distinct().count()
                    print(f"  {feature:20s}: {distinct_count} unique values")
        
        print("="*80 + "\n")


def test_data_loader(spark: SparkSession):
    """Test the data loader"""
    loader = WeatherDataLoader(spark)
    
    # Load data
    df = loader.load_data(limit_rows=1000)
    
    # Validate
    validation = loader.validate_data(df)
    
    # Summary
    loader.summary_stats(df)
    
    # Show sample
    print("Sample data:")
    df.select(config.ALL_TARGET_FEATURES + ['datetime']).show(5, truncate=False)
    
    return df, validation


if __name__ == "__main__":
    # This is for testing only
    print("Testing data loader...")
    print("Note: Requires Spark session with MinIO configuration")
