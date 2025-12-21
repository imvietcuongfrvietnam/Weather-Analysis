import redis
import json
import os

class RedisWriter:
    def __init__(self):
        # 1. Lấy cấu hình từ biến môi trường (Được set trong file Airflow DAG)
        # Nếu chạy local thì fallback về localhost
        self.redis_host = os.getenv("REDIS_HOST", "weather-redis.default.svc.cluster.local")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_db = 0
        self.redis_password = os.getenv("REDIS_PASSWORD", None)

    def _get_redis_connection(self):
        """Tạo kết nối Redis an toàn"""
        return redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            decode_responses=True # Nhận string thay vì bytes
        )

    def write_stream_to_redis(self, batch_df, batch_id):
        """
        Hàm này được Spark gọi mỗi khi có Batch dữ liệu mới (5 giây/lần)
        """
        # Nếu batch rỗng thì bỏ qua ngay để tiết kiệm tài nguyên
        if batch_df.isEmpty():
            return

        print(f"⚡ [Redis] Đang ghi batch {batch_id}...")

        # Convert Spark DataFrame -> List Dictionary
        records = batch_df.collect()
        
        try:
            r = self._get_redis_connection()
            pipe = r.pipeline() # Dùng pipeline tăng tốc độ ghi
            
            count = 0
            for row in records:
                # Chuyển Row thành Dictionary
                data = row.asDict()
                
                # --- QUAN TRỌNG: Xử lý dữ liệu ---
                
                # 1. Convert Timestamp/Date thành String (JSON không hiểu object thời gian của Python)
                if 'datetime' in data and data['datetime']:
                    data['datetime'] = str(data['datetime'])
                
                # 2. Tạo Key chuẩn: weather:current:{Tên_Thành_Phố}
                if 'city' in data:
                    city_key = data['city'].strip()
                    redis_key = f"weather:current:{city_key}"
                    
                    # 3. Ghi dữ liệu dạng JSON String
                    # Lý do: Dùng JSON linh hoạt hơn HSET vì lỡ sau này bạn thêm cột 'humidity' 
                    # thì code này tự động lưu luôn mà không cần sửa code.
                    pipe.set(redis_key, json.dumps(data))
                    
                    # (Tùy chọn) Set thời gian hết hạn là 1 tiếng (3600s) để tránh rác DB
                    pipe.expire(redis_key, 3600)
                    
                    count += 1
            
            # Thực thi lệnh
            pipe.execute()
            print(f"✅ [Redis] Đã cập nhật {count} thành phố.")
            
        except Exception as e:
            print(f"❌ [Redis Error] Lỗi ghi dữ liệu: {e}")