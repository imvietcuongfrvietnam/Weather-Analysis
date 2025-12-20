import os

# Lấy từ biến môi trường (K8s Service), mặc định là localhost (cho test ngoài)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None  # Nếu sau này có pass thì dùng os.getenv("REDIS_PASSWORD")

REDIS_KEY_PREFIX = "weather:current"
REDIS_TTL = 3600