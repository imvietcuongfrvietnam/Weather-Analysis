"""
Script hỗ trợ: Tạo bảng PostgreSQL cho forecasts
Chạy script này một lần để tạo bảng trước khi chạy LSTM forecast
"""

import sys
import os

# Thêm đường dẫn để import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'spark_etl_weather_disaster'))

import postgres_config

def create_table():
    """Tạo bảng PostgreSQL"""
    try:
        import psycopg2
        
        print("🔗 Kết nối PostgreSQL...")
        conn = psycopg2.connect(
            host=postgres_config.POSTGRES_HOST,
            port=postgres_config.POSTGRES_PORT,
            database=postgres_config.POSTGRES_DATABASE,
            user=postgres_config.POSTGRES_USER,
            password=postgres_config.POSTGRES_PASSWORD
        )
        
        print("✅ Đã kết nối thành công!")
        print("\n📋 Tạo bảng và indexes...")
        
        cursor = conn.cursor()
        cursor.execute(postgres_config.FORECAST_TABLE_SCHEMA)
        conn.commit()
        
        # Kiểm tra bảng đã được tạo
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'weather_forecasts'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print("✅ Bảng 'weather_forecasts' đã được tạo thành công!")
            
            # Hiển thị schema
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'weather_forecasts'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            print("\n📊 Schema bảng:")
            for col_name, data_type in columns:
                print(f"   - {col_name}: {data_type}")
        else:
            print("⚠️  Bảng có thể chưa được tạo. Kiểm tra lại.")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Hoàn tất!")
        
    except ImportError:
        print("❌ psycopg2 không được cài đặt!")
        print("💡 Cài đặt: pip install psycopg2-binary")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        print("\n💡 Kiểm tra:")
        print("   1. PostgreSQL server đang chạy")
        print("   2. Database và user đã được tạo")
        print("   3. Credentials trong postgres_config.py đúng")
        sys.exit(1)

if __name__ == "__main__":
    print("="*80)
    print("🔧 SETUP POSTGRESQL TABLE FOR WEATHER FORECASTS")
    print("="*80)
    print(f"\n📊 Database: {postgres_config.POSTGRES_DATABASE}")
    print(f"📋 Table: {postgres_config.FORECAST_TABLE}")
    print("="*80 + "\n")
    
    create_table()

