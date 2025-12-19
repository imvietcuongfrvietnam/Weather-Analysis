"""
PostgreSQL Connector
Kết nối và lấy dữ liệu dự báo từ PostgreSQL
"""

import psycopg2
from psycopg2 import pool
import pandas as pd
from typing import Optional, List
import config

class PostgresConnector:
    """Connect to PostgreSQL and fetch forecast data"""
    
    def __init__(self):
        self.host = config.POSTGRES_HOST
        self.port = config.POSTGRES_PORT
        self.database = config.POSTGRES_DB
        self.user = config.POSTGRES_USER
        self.password = config.POSTGRES_PASSWORD
        self.table = config.POSTGRES_TABLE
        self.connection_pool = None
        
    def connect(self) -> bool:
        """
        Create connection pool to PostgreSQL
        
        Returns:
            bool: True if connected successfully
        """
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            if self.connection_pool:
                # Test connection
                conn = self.connection_pool.getconn()
                conn.close()
                self.connection_pool.putconn(conn)
                return True
            
            return False
            
        except psycopg2.OperationalError as e:
            print(f"❌ PostgreSQL connection error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    def get_forecast(self, city: str = None, days: int = 7) -> Optional[pd.DataFrame]:
        """
        Get forecast data for next N days
        
        Args:
            city: City name (optional filter)
            days: Number of days to forecast (default: 7)
            
        Returns:
            DataFrame with forecast data or None
        """
        if not self.connection_pool:
            if not self.connect():
                return None
        
        try:
            conn = self.connection_pool.getconn()
            
            # Build query
            query = f"""
                SELECT 
                    datetime,
                    city,
                    temp_celsius,
                    prediction_temp_celsius,
                    humidity_pct,
                    prediction_humidity_pct,
                    pressure_hpa,
                    prediction_pressure_hpa,
                    wind_speed_kmh,
                    prediction_wind_speed_kmh,
                    precipitation_mm,
                    prediction_precipitation_mm,
                    weather_condition,
                    prediction_weather_condition,
                    created_at
                FROM {self.table}
                WHERE datetime > NOW()
            """
            
            if city:
                query += f" AND city = '{city}'"
            
            query += f"""
                ORDER BY datetime ASC
                LIMIT {days * 24}
            """
            
            # Execute query
            df = pd.read_sql_query(query, conn)
            
            # Return connection to pool
            self.connection_pool.putconn(conn)
            
            return df if not df.empty else None
            
        except Exception as e:
            print(f"❌ Error fetching forecast: {e}")
            if conn:
                self.connection_pool.putconn(conn)
            return None
    
    def get_latest_predictions(self, city: str = None, limit: int = 168) -> Optional[pd.DataFrame]:
        """
        Get latest predictions (most recently created)
        
        Args:
            city: City name filter
            limit: Number of records (default: 168 = 7 days × 24 hours)
            
        Returns:
            DataFrame with predictions
        """
        if not self.connection_pool:
            if not self.connect():
                return None
        
        try:
            conn = self.connection_pool.getconn()
            
            query = f"""
                SELECT *
                FROM {self.table}
            """
            
            if city:
                query += f" WHERE city = '{city}'"
            
            query += f"""
                ORDER BY created_at DESC, datetime DESC
                LIMIT {limit}
            """
            
            df = pd.read_sql_query(query, conn)
            self.connection_pool.putconn(conn)
            
            return df if not df.empty else None
            
        except Exception as e:
            print(f"❌ Error getting predictions: {e}")
            if conn:
                self.connection_pool.putconn(conn)
            return None
    
    def get_accuracy_metrics(self, city: str = None) -> Optional[pd.DataFrame]:
        """
        Calculate prediction accuracy metrics
        
        Args:
            city: City name filter
            
        Returns:
            DataFrame with MAE, RMSE for each feature
        """
        if not self.connection_pool:
            if not self.connect():
                return None
        
        try:
            conn = self.connection_pool.getconn()
            
            # Calculate metrics where we have both actual and predicted
            query = f"""
                SELECT 
                    AVG(ABS(temp_celsius - prediction_temp_celsius)) as mae_temp,
                    AVG(ABS(humidity_pct - prediction_humidity_pct)) as mae_humidity,
                    AVG(ABS(pressure_hpa - prediction_pressure_hpa)) as mae_pressure,
                    AVG(ABS(wind_speed_kmh - prediction_wind_speed_kmh)) as mae_wind,
                    AVG(ABS(precipitation_mm - prediction_precipitation_mm)) as mae_precip,
                    COUNT(*) as sample_count
                FROM {self.table}
                WHERE temp_celsius IS NOT NULL 
                  AND prediction_temp_celsius IS NOT NULL
            """
            
            if city:
                query += f" AND city = '{city}'"
            
            df = pd.read_sql_query(query, conn)
            self.connection_pool.putconn(conn)
            
            return df if not df.empty else None
            
        except Exception as e:
            print(f"❌ Error calculating metrics: {e}")
            if conn:
                self.connection_pool.putconn(conn)
            return None
    
    def get_available_cities(self) -> List[str]:
        """
        Get list of cities in database
        
        Returns:
            List of city names
        """
        if not self.connection_pool:
            if not self.connect():
                return []
        
        try:
            conn = self.connection_pool.getconn()
            
            query = f"SELECT DISTINCT city FROM {self.table} ORDER BY city"
            df = pd.read_sql_query(query, conn)
            
            self.connection_pool.putconn(conn)
            
            return df['city'].tolist() if not df.empty else []
            
        except Exception as e:
            print(f"❌ Error getting cities: {e}")
            if conn:
                self.connection_pool.putconn(conn)
            return []
    
    def is_connected(self) -> bool:
        """Check if connected to PostgreSQL"""
        if not self.connection_pool:
            return False
        
        try:
            conn = self.connection_pool.getconn()
            self.connection_pool.putconn(conn)
            return True
        except:
            return False
    
    def close(self):
        """Close all connections"""
        if self.connection_pool:
            self.connection_pool.closeall()


def test_connection():
    """Test PostgreSQL connection"""
    print("🧪 Testing PostgreSQL connection...")
    
    connector = PostgresConnector()
    
    if connector.connect():
        print("✅ Connected to PostgreSQL!")
        
        # Get cities
        cities = connector.get_available_cities()
        print(f"📍 Cities: {cities}")
        
        # Get forecast
        forecast = connector.get_forecast()
        if forecast is not None:
            print(f"✅ Got {len(forecast)} forecast records")
            print(forecast.head())
        else:
            print("⚠️  No forecast data yet")
        
        connector.close()
    else:
        print("❌ Could not connect to PostgreSQL")
        print("💡 Make sure PostgreSQL server is running and configured")


if __name__ == "__main__":
    test_connection()
