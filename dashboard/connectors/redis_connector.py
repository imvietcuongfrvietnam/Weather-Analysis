"""
Redis Connector
Kết nối và lấy dữ liệu real-time từ Redis
"""

import redis
import json
from datetime import datetime
from typing import Dict, Optional, List
import config

class RedisConnector:
    """Connect to Redis and fetch real-time weather data"""
    
    def __init__(self):
        self.host = config.REDIS_HOST
        self.port = config.REDIS_PORT
        self.db = config.REDIS_DB
        self.password = config.REDIS_PASSWORD
        self.key_prefix = config.REDIS_KEY_PREFIX
        self.client = None
        
    def connect(self) -> bool:
        """
        Connect to Redis server
        
        Returns:
            bool: True if connected successfully
        """
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            
            # Test connection
            self.client.ping()
            return True
            
        except redis.ConnectionError as e:
            print(f"❌ Redis connection error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error connecting to Redis: {e}")
            return False
    
    def get_current_weather(self, city: str = None) -> Optional[Dict]:
        """
        Get current weather data for a city
        
        Args:
            city: City name (optional, uses all if None)
            
        Returns:
            Dict with weather data or None if not found
        """
        if not self.client:
            if not self.connect():
                return None
        
        try:
            # If city specified, get specific key
            if city:
                key = f"{self.key_prefix}:{city}"
                data = self.client.get(key)
                
                if data:
                    return json.loads(data)
                else:
                    return None
            
            # Otherwise get all weather data
            pattern = f"{self.key_prefix}:*"
            keys = self.client.keys(pattern)
            
            if not keys:
                return None
            
            # Get the first available city's data
            data = self.client.get(keys[0])
            if data:
                return json.loads(data)
            
            return None
            
        except redis.RedisError as e:
            print(f"❌ Redis error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            return None
        except Exception as e:
            print(f"❌ Error fetching data: {e}")
            return None
    
    def get_all_cities(self) -> List[str]:
        """
        Get list of all cities in Redis
        
        Returns:
            List of city names
        """
        if not self.client:
            if not self.connect():
                return []
        
        try:
            pattern = f"{self.key_prefix}:*"
            keys = self.client.keys(pattern)
            
            # Extract city names from keys
            cities = [key.split(":")[-1] for key in keys]
            return sorted(cities)
            
        except Exception as e:
            print(f"❌ Error getting cities: {e}")
            return []
    
    def get_weather_history(self, city: str, hours: int = 1) -> List[Dict]:
        """
        Get weather history for the last N hours
        (This assumes Redis stores timestamped data or uses sorted sets)
        
        Args:
            city: City name
            hours: Number of hours of history
            
        Returns:
            List of weather data points
        """
        # Note: This implementation assumes data is stored with timestamps
        # Adjust based on actual Redis data structure
        if not self.client:
            if not self.connect():
                return []
        
        try:
            # For demo, we'll return current data only
            # In production, implement proper time-series storage
            current = self.get_current_weather(city)
            if current:
                return [current]
            return []
            
        except Exception as e:
            print(f"❌ Error getting history: {e}")
            return []
    
    def is_connected(self) -> bool:
        """
        Check if Redis is connected
        
        Returns:
            bool: True if connected
        """
        if not self.client:
            return False
        
        try:
            self.client.ping()
            return True
        except:
            return False
    
    def close(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()


def test_connection():
    """Test Redis connection"""
    print("🧪 Testing Redis connection...")
    
    connector = RedisConnector()
    
    if connector.connect():
        print("✅ Connected to Redis!")
        
        # Get current weather
        weather = connector.get_current_weather()
        if weather:
            print(f"✅ Got weather data: {weather}")
        else:
            print("⚠️  No weather data in Redis yet")
        
        # Get cities
        cities = connector.get_all_cities()
        print(f"📍 Cities in Redis: {cities}")
        
        connector.close()
    else:
        print("❌ Could not connect to Redis")
        print("💡 Make sure Redis server is running:")
        print("   docker run -d -p 6379:6379 redis:latest")


if __name__ == "__main__":
    test_connection()
