"""
PostgreSQL Writer - Ghi dữ liệu dự đoán vào PostgreSQL
Save weather forecast predictions to PostgreSQL database
"""

from pyspark.sql import DataFrame
import config

class PostgresWriter:
    """Write forecast predictions to PostgreSQL database"""
    
    def __init__(self):
        self.jdbc_url = config.POSTGRES_JDBC_URL
        self.properties = config.POSTGRES_PROPERTIES
        self.table = config.POSTGRES_TABLE
        self.write_mode = config.POSTGRES_WRITE_MODE
        
    def write_predictions(self, df: DataFrame, table_name: str = None):
        """
        Write predictions DataFrame to PostgreSQL
        
        Args:
            df: Spark DataFrame with predictions
            table_name: Table name (optional, uses config if not provided)
        """
        table = table_name or self.table
        
        print(f"\n💾 Writing predictions to PostgreSQL...")
        print(f"   JDBC URL: {self.jdbc_url}")
        print(f"   Table: {table}")
        print(f"   Mode: {self.write_mode}")
        
        try:
            # Write to PostgreSQL using JDBC
            df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table=table,
                    mode=self.write_mode,
                    properties=self.properties
                )
            
            record_count = df.count()
            print(f"   ✅ Successfully wrote {record_count} records to PostgreSQL")
            
            return True
            
        except Exception as e:
            print(f"   ⚠️  Could not write to PostgreSQL: {e}")
            print(f"   💡 This is expected if PostgreSQL server is not running yet")
            print(f"   💡 Predictions are still saved to CSV file")
            return False
    
    def write_predictions_safe(self, df: DataFrame, table_name: str = None):
        """
        Write predictions with error handling (doesn't fail if PostgreSQL unavailable)
        
        Args:
            df: Spark DataFrame with predictions
            table_name: Table name (optional)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.write_predictions(df, table_name)
        except Exception as e:
            print(f"\n⚠️  PostgreSQL write failed (this is OK if server not set up yet)")
            print(f"   Error: {e}")
            return False
    
    def create_table_schema(self) -> str:
        """
        Generate PostgreSQL table creation SQL
        
        Returns:
            str: SQL CREATE TABLE statement
        """
        sql = f"""
CREATE TABLE IF NOT EXISTS {self.table} (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    city VARCHAR(100),
    
    -- Actual values
    temp_celsius DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    weather_condition VARCHAR(50),
    
    -- Predicted values
    prediction_temp_celsius DOUBLE PRECISION,
    prediction_humidity_pct DOUBLE PRECISION,
    prediction_pressure_hpa DOUBLE PRECISION,
    prediction_wind_speed_kmh DOUBLE PRECISION,
    prediction_precipitation_mm DOUBLE PRECISION,
    prediction_weather_condition VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_datetime (datetime),
    INDEX idx_city (city)
);
        """
        return sql.strip()
    
    def print_setup_instructions(self):
        """Print instructions for setting up PostgreSQL"""
        print("\n" + "="*80)
        print("🐘 POSTGRESQL SETUP INSTRUCTIONS")
        print("="*80)
        
        print("\n1. Install PostgreSQL:")
        print("   # Ubuntu/Debian")
        print("   sudo apt-get install postgresql postgresql-contrib")
        print("\n   # macOS")
        print("   brew install postgresql")
        print("\n   # Docker")
        print("   docker run -d --name weather-postgres \\")
        print("     -e POSTGRES_PASSWORD=postgres \\")
        print("     -e POSTGRES_DB=weather_forecast \\")
        print("     -p 5432:5432 \\")
        print("     postgres:15")
        
        print("\n2. Create Database:")
        print("   psql -U postgres -c 'CREATE DATABASE weather_forecast;'")
        
        print("\n3. Create Table:")
        print("   psql -U postgres -d weather_forecast <<EOF")
        print(self.create_table_schema())
        print("   EOF")
        
        print("\n4. Set Environment Variables (Optional):")
        print("   export POSTGRES_HOST=localhost")
        print("   export POSTGRES_PORT=5432")
        print("   export POSTGRES_DB=weather_forecast")
        print("   export POSTGRES_USER=postgres")
        print("   export POSTGRES_PASSWORD=your_password")
        print("   export POSTGRES_TABLE=weather_predictions")
        
        print("\n5. Install JDBC Driver:")
        print("   # The system will auto-download via Spark packages:")
        print("   org.postgresql:postgresql:42.6.0")
        
        print("\n" + "="*80)
        print("💡 TIP: Sau khi setup PostgreSQL, chạy lại weather_forecasting.py")
        print("        Hệ thống sẽ tự động ghi dữ liệu vào database")
        print("="*80 + "\n")
    
    def test_connection(self):
        """Test PostgreSQL connection"""
        print(f"\n🧪 Testing PostgreSQL connection...")
        print(f"   Host: {config.POSTGRES_HOST}:{config.POSTGRES_PORT}")
        print(f"   Database: {config.POSTGRES_DB}")
        print(f"   User: {config.POSTGRES_USER}")
        
        try:
            # Try to connect using psycopg2 (if available)
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=config.POSTGRES_HOST,
                    port=config.POSTGRES_PORT,
                    database=config.POSTGRES_DB,
                    user=config.POSTGRES_USER,
                    password=config.POSTGRES_PASSWORD
                )
                conn.close()
                print("   ✅ PostgreSQL connection successful!")
                return True
            except ImportError:
                print("   💡 psycopg2 not installed (optional for testing)")
                print("   💡 To test: pip install psycopg2-binary")
                print("   💡 Spark will use JDBC driver instead")
                return None
                
        except Exception as e:
            print(f"   ⚠️  Cannot connect to PostgreSQL: {e}")
            print(f"   💡 This is OK if PostgreSQL is not set up yet")
            return False


def print_table_creation_sql():
    """Print SQL for table creation"""
    writer = PostgresWriter()
    print("\n" + "="*80)
    print("📝 POSTGRESQL TABLE CREATION SQL")
    print("="*80 + "\n")
    print(writer.create_table_schema())
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    # Print setup instructions
    writer = PostgresWriter()
    writer.print_setup_instructions()
    
    # Print table creation SQL
    print_table_creation_sql()
    
    # Test connection
    writer.test_connection()
