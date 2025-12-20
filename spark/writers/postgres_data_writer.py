"""
PostgreSQL Data Writer
Ghi dữ liệu dự đoán từ SparkML vào PostgreSQL
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import postgres_config


class PostgresWriter:
    """
    Writer để ghi dữ liệu vào PostgreSQL
    """
    
    def __init__(self):
        """Khởi tạo PostgreSQL writer"""
        self.jdbc_config = postgres_config.get_spark_jdbc_config()
        self.jdbc_props = postgres_config.get_spark_jdbc_properties()
    
    def write_forecasts(self, df: DataFrame, table_name: str = None):
        """
        Ghi dự đoán thời tiết vào PostgreSQL
        
        Args:
            df: Spark DataFrame chứa dự đoán
            table_name: Tên bảng (mặc định: FORECAST_TABLE từ config)
        """
        if table_name is None:
            table_name = postgres_config.FORECAST_TABLE
        
        print(f"\n💾 Ghi dự đoán vào PostgreSQL...")
        print(f"   📊 Số lượng records: {df.count()}")
        print(f"   📋 Table: {table_name}")
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", self.jdbc_config['url']) \
                .option("dbtable", table_name) \
                .option("user", self.jdbc_props['user']) \
                .option("password", self.jdbc_props['password']) \
                .option("driver", self.jdbc_props['driver']) \
                .mode("append") \
                .save()
            
            print(f"✅ Đã ghi thành công vào {table_name}!")
            
        except Exception as e:
            print(f"❌ Lỗi ghi vào PostgreSQL: {e}")
            raise
    
    def write_forecasts_overwrite(self, df: DataFrame, table_name: str = None):
        """
        Ghi dự đoán với mode overwrite (xóa dữ liệu cũ)
        
        Args:
            df: Spark DataFrame chứa dự đoán
            table_name: Tên bảng
        """
        if table_name is None:
            table_name = postgres_config.FORECAST_TABLE
        
        print(f"\n💾 Ghi dự đoán vào PostgreSQL (overwrite mode)...")
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", self.jdbc_config['url']) \
                .option("dbtable", table_name) \
                .option("user", self.jdbc_props['user']) \
                .option("password", self.jdbc_props['password']) \
                .option("driver", self.jdbc_config['driver']) \
                .mode("overwrite") \
                .save()
            
            print(f"✅ Đã ghi thành công (overwrite) vào {table_name}!")
            
        except Exception as e:
            print(f"❌ Lỗi ghi vào PostgreSQL: {e}")
            raise
    
    def create_table_if_not_exists(self, spark):
        """
        Tạo bảng PostgreSQL nếu chưa tồn tại
        
        Args:
            spark: SparkSession
        """
        print(f"\n🔧 Kiểm tra và tạo bảng PostgreSQL...")
        
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=postgres_config.POSTGRES_HOST,
                port=postgres_config.POSTGRES_PORT,
                database=postgres_config.POSTGRES_DATABASE,
                user=postgres_config.POSTGRES_USER,
                password=postgres_config.POSTGRES_PASSWORD
            )
            
            cursor = conn.cursor()
            cursor.execute(postgres_config.FORECAST_TABLE_SCHEMA)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            print(f"✅ Bảng {postgres_config.FORECAST_TABLE} đã sẵn sàng!")
            
        except ImportError:
            print("⚠️  psycopg2 không được cài đặt. Bỏ qua việc tạo bảng.")
            print("💡 Cài đặt: pip install psycopg2-binary")
        except Exception as e:
            print(f"⚠️  Lỗi tạo bảng: {e}")
            print("💡 Có thể bảng đã tồn tại hoặc cần quyền admin")

