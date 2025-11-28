"""
Dashboard Configuration
Cấu hình kết nối Redis và PostgreSQL
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==========================================
# REDIS CONFIGURATION
# ==========================================
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
REDIS_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "weather:current")

# ==========================================
# POSTGRESQL CONFIGURATION
# ==========================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")             # Khớp YAML file
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")       # Khớp YAML file
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass") # Khớp YAML file
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")
# DASHBOARD SETTINGS
# ==========================================
DASHBOARD_REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "10"))
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8501"))
DASHBOARD_TITLE = "🌦️ Weather Forecast Dashboard"

# Cities to display (can filter from data)
DEFAULT_CITY = os.getenv("DEFAULT_CITY", "New York")

# Chart settings
CHART_HEIGHT = 400
CHART_WIDTH = 800

# Temperature unit
TEMP_UNIT = os.getenv("TEMP_UNIT", "celsius")  # celsius or fahrenheit

def print_config():
    """Print current configuration"""
    print("=" * 80)
    print("⚙️  DASHBOARD CONFIGURATION")
    print("=" * 80)
    print(f"Redis:        {REDIS_HOST}:{REDIS_PORT} (DB {REDIS_DB})")
    print(f"PostgreSQL:   {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"  Table:      {POSTGRES_TABLE}")
    print(f"Refresh Rate: {DASHBOARD_REFRESH_SECONDS} seconds")
    print(f"Default City: {DEFAULT_CITY}")
    print("=" * 80)

if __name__ == "__main__":
    print_config()
