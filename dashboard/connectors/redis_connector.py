"""
Redis Connector
Kết nối và lấy dữ liệu real-time từ Redis
"""

import redis
import json
import sys
import os
from typing import Dict, Optional, List

# --- SETUP IMPORT CONFIG ---
# Thêm thư mục cha (dashboard/) vào sys.path để import config.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import config
except ImportError:
    # Fallback nếu chạy trực tiếp file này mà không qua app.py
    class Config:
        REDIS_HOST = "localhost"
        REDIS_PORT = 6379
        REDIS_DB = 0
        REDIS_PASSWORD = None
        REDIS_KEY_PREFIX = "weather:current"
    config = Config()

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
        """Connect to Redis server"""
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
            self.client.ping()
            return True
        except Exception as e:
            print(f"❌ Redis connection error: {e}")
            return False
    
    def get_current_weather(self, city: str = None) -> Optional[Dict]:
        """Get current weather data for a city"""
        if not self.client and not self.connect():
            return None
        
        try:
            if city:
                key = f"{self.key_prefix}:{city}"
                data = self.client.get(key)
                return json.loads(data) if data else None
            
            # Get all
            pattern = f"{self.key_prefix}:*"
            keys = self.client.keys(pattern)
            if not keys: return None
            
            # Demo: lấy cái đầu tiên
            data = self.client.get(keys[0])
            return json.loads(data) if data else None
            
        except Exception as e:
            print(f"❌ Redis error: {e}")
            return None
    
    def get_all_cities(self) -> List[str]:
        """Get list of all cities in Redis"""
        if not self.client and not self.connect():
            return []
        try:
            pattern = f"{self.key_prefix}:*"
            keys = self.client.keys(pattern)
            cities = [key.split(":")[-1] for key in keys]
            return sorted(cities)
        except Exception:
            return []
    def close(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()
if __name__ == "__main__":
    print(f"🧪 Testing Redis connection to {config.REDIS_HOST}:{config.REDIS_PORT}...")
    connector = RedisConnector()
    if connector.connect():
        print("✅ Connected!")
        print("Cities:", connector.get_all_cities())
    else:
        print("❌ Failed.")