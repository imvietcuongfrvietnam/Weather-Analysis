from pyspark.sql import SparkSession, DataFrame
import os
import sys

# Import trực tiếp vì file nằm cùng folder job hoặc đã được thêm vào sys.path
try:
    import minio_config
except ImportError:
    from config import minio_config

class MinIOWriter:
    """
    Class chuyên trách ghi dữ liệu xuống MinIO (Data Lake)
    Sử dụng giao thức S3A của Spark.
    """
    
    def __init__(self):
        self.bucket = minio_config.MINIO_BUCKET
        print(f"📦 MinIO Writer initialized for bucket: {self.bucket}")

    def write_stream(self, df: DataFrame, folder: str = "enriched", trigger_time="30 seconds"):
        """
        Ghi luồng dữ liệu (Streaming) xuống MinIO định dạng Parquet.
        
        Args:
            df: DataFrame cần ghi
            folder: Tên folder lưu trữ (vd: 'enriched', 'cleaned')
            trigger_time: Chu kỳ ghi (vd: '30 seconds', '1 minute')
        """
        # 1. Tạo đường dẫn file (Path)
        output_path = f"s3a://{self.bucket}/{folder}/weather"
        
        # 2. Tạo đường dẫn Checkpoint
        # Sử dụng /tmp để đảm bảo quyền ghi trong môi trường K8s nếu S3A checkpoint bị lỗi
        checkpoint_path = f"/tmp/checkpoints/weather_{folder}"
        
        print(f"\n💾 [MinIO] Config Streaming Write:")
        print(f"   👉 Output:     {output_path}")
        print(f"   👉 Checkpoint: {checkpoint_path}")
        
        # 3. Khởi tạo Query Streaming
        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime=trigger_time) \
            .queryName(f"WriteToMinIO_{folder}") \
            .start()
            
        return query