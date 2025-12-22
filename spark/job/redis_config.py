import os

# SỬA TẠI ĐÂY: Dùng FQDN để gọi từ namespace 'airflow' sang namespace 'default'
# Cấu trúc: [SERVICE_NAME].[NAMESPACE].svc.cluster.local
REDIS_HOST = os.getenv("REDIS_HOST", "weather-redis.default.svc.cluster.local")

REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

REDIS_KEY_PREFIX = "weather:current"
REDIS_TTL = 3600  # Dữ liệu sẽ hết hạn sau 1 giờ

def print_config():
    """In ra cấu hình hiện tại để debug"""
    print("\n" + "="*80)
    print("🚀 REDIS CONFIGURATION")
    print("="*80)
    print(f"Host:     {REDIS_HOST}")
    print(f"Port:     {REDIS_PORT}")
    print(f"Prefix:   {REDIS_KEY_PREFIX}")
    print(f"DB:       {REDIS_DB}")
    print("="*80 + "\n")

if __name__ == "__main__":
    print_config()