"""
Real Data Writers
Write data to JSON files (for testing), MinIO (S3-compatible storage), HDFS (for storage), or Elasticsearch (for indexing)
"""

from pyspark.sql import DataFrame
import os
from minio import Minio
from minio.error import S3Error
import io
import pandas as pd


class DataWriter:
    """
    Data writer with multiple destinations:
    - JSON files (for testing and viewing cleaned data)
    - MinIO (S3-compatible storage for production)
    - HDFS (for distributed storage)
    - Elasticsearch (for search and analytics)
    """
    """
    
    def __init__(self, output_type: str = "json"):
        """
        Args:
            output_type: "json", "minio", "hdfs", or "elasticsearch"
        """
        self.output_type = output_type
        self.output_dir = "./output"
        self.minio_client = None
        
        # Initialize MinIO client if needed
        if output_type == "minio":
            self._init_minio_client()
        
    def _init_minio_client(self):
        """
        Initialize MinIO client with configuration
        """
        try:
            # Import config
            from minio_config import (
                MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, 
                MINIO_SECURE, MINIO_BUCKET, print_config
            )
            
            # Print configuration
            print_config()
            
            # Create MinIO client
            self.minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE
            )
            
            # Check if bucket exists, create if not
            if not self.minio_client.bucket_exists(MINIO_BUCKET):
                print(f"📦 Creating bucket: {MINIO_BUCKET}")
                self.minio_client.make_bucket(MINIO_BUCKET)
                print(f"   ✅ Bucket created successfully")
            else:
                print(f"   ✅ Bucket '{MINIO_BUCKET}' already exists")
            
            self.minio_bucket = MINIO_BUCKET
            
        except ImportError:
            print("⚠️  minio_config.py not found. Please create configuration file.")
            print("   Running in JSON-only mode.")
            self.output_type = "json"
        except Exception as e:
            print(f"⚠️  Could not connect to MinIO: {e}")
            print(f"   💡 Make sure MinIO server is running")
            print(f"   💡 Falling back to JSON mode")
            self.output_type = "json"
    def write_cleaned_data(self, df: DataFrame, dataset_name: str):
        """
        Write cleaned data to configured destination
        
        Args:
            df: DataFrame to write
            dataset_name: Name of dataset (e.g., "weather", "311_requests", "taxi_trips", "collisions")
        """
        print(f"\n💾 Writing {dataset_name} data to {self.output_type}...")
        
        if self.output_type == "json":
            self._write_to_json(df, dataset_name, folder="cleaned")
        elif self.output_type == "minio":
            self._write_to_minio(df, dataset_name, folder="cleaned")
        elif self.output_type == "hdfs":
            self._write_to_hdfs(df, dataset_name)
        elif self.output_type == "elasticsearch":
            self._write_to_elasticsearch(df, dataset_name)
        else:
            raise ValueError(f"Unknown output type: {self.output_type}")
    
    def write_enriched_data(self, df: DataFrame, dataset_name: str = "integrated"):
        """
        Write final enriched/integrated data
        
        Args:
            df: Final integrated DataFrame
            dataset_name: Name for output (default: "integrated")
        """
        print(f"\n💾 Writing final {dataset_name} data to {self.output_type}...")
        
        if self.output_type == "json":
            self._write_to_json(df, f"{dataset_name}_final", folder="enriched")
        elif self.output_type == "minio":
            self._write_to_minio(df, dataset_name, folder="enriched")
        elif self.output_type == "hdfs":
            self._write_to_hdfs(df, f"{dataset_name}_final")
        elif self.output_type == "elasticsearch":
            self._write_to_elasticsearch(df, f"{dataset_name}_final")
        else:
            raise ValueError(f"Unknown output type: {self.output_type}")
    
    # ============================================
    # Private methods for different destinations
    # ============================================
    
    def _write_to_json(self, df: DataFrame, dataset_name: str, folder: str = ""):
        """
        Write to JSON files using pandas (Windows compatible, no Hadoop needed)
        This allows viewing cleaned data easily
        """
        # Create output directory
        if folder:
            output_folder = os.path.join(self.output_dir, folder)
        else:
            output_folder = self.output_dir
            
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, f"{dataset_name}_cleaned.json")
        
        try:
            # Convert to pandas and save as JSON
            # Use orient='records' for readable JSON array format
            pandas_df = df.toPandas()
            pandas_df.to_json(output_path, orient='records', indent=2, date_format='iso')
            
            record_count = df.count()
            print(f"   ✅ Saved {record_count} records to: {output_path}")
            print(f"   📊 Columns: {len(df.columns)}")
            print(f"   📁 File size: {os.path.getsize(output_path) / 1024:.2f} KB")
            
        except Exception as e:
            print(f"   ⚠️  Could not save to JSON: {str(e)}")
            print(f"   📊 Showing sample data instead:")
            df.show(5, truncate=False)
    
    def _write_to_minio(self, df: DataFrame, dataset_name: str, folder: str = "cleaned"):
        """
        Write to MinIO using Spark S3A or MinIO Python client
        
        Args:
            df: DataFrame to write
            dataset_name: Name of dataset
            folder: Folder in bucket (cleaned, enriched, etc.)
        """
        if not self.minio_client:
            print(f"   ⚠️  MinIO client not initialized. Falling back to JSON.")
            self._write_to_json(df, dataset_name, folder)
            return
        
        try:
            # Convert to pandas for easier MinIO upload
            # For large datasets, consider using Spark S3A directly
            pandas_df = df.toPandas()
            
            # Convert to Parquet in memory
            parquet_buffer = io.BytesIO()
            pandas_df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_buffer.seek(0)
            
            # Object name: folder/dataset_name/data.parquet
            object_name = f"{folder}/{dataset_name}/data.parquet"
            
            # Upload to MinIO
            parquet_size = parquet_buffer.getbuffer().nbytes
            self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_size,
                content_type='application/octet-stream'
            )
            
            record_count = len(pandas_df)
            print(f"   ✅ Uploaded {record_count} records to MinIO")
            print(f"   📦 Bucket: {self.minio_bucket}")
            print(f"   📁 Path: {object_name}")
            print(f"   📊 Size: {parquet_size / 1024:.2f} KB")
            print(f"   🔗 S3 URI: s3a://{self.minio_bucket}/{object_name}")
            
        except S3Error as e:
            print(f"   ❌ MinIO error: {e}")
            print(f"   💡 Falling back to JSON mode")
            self._write_to_json(df, dataset_name, folder)
        except Exception as e:
            print(f"   ❌ Error uploading to MinIO: {e}")
            print(f"   💡 Falling back to JSON mode")
            self._write_to_json(df, dataset_name, folder)
    
    def _write_to_minio_spark(self, df: DataFrame, dataset_name: str, folder: str = "cleaned"):
        """
        Alternative: Write to MinIO using Spark S3A (for large datasets)
        Requires Spark to be configured with S3A connector
        
        NOTE: This method requires additional Spark configuration in main_etl.py
        """
        from minio_config import MINIO_BUCKET
        
        # S3A path
        s3_path = f"s3a://{MINIO_BUCKET}/{folder}/{dataset_name}"
        
        try:
            # Write as Parquet (recommended for big data)
            df.write \
                .mode("overwrite") \
                .format("parquet") \
                .save(s3_path)
            
            print(f"   ✅ Saved to MinIO via Spark S3A: {s3_path}")
            print(f"   📊 Records: {df.count()}")
            
        except Exception as e:
            print(f"   ❌ Spark S3A write failed: {e}")
            print(f"   💡 Make sure Spark is configured with S3A connector")
            print(f"   💡 Falling back to Python MinIO client")
            self._write_to_minio(df, dataset_name, folder)
    
    # ============================================
    # Streaming Write Methods (for Kafka streaming mode)
    # ============================================
    
    def write_cleaned_data_streaming(self, df: DataFrame, dataset_name: str, checkpoint_location: str = None):
        """
        Write streaming cleaned data to configured destination
        
        Args:
            df: Streaming DataFrame to write
            dataset_name: Name of dataset (e.g., "weather", "311_requests")
            checkpoint_location: Path for checkpoint (required for streaming)
        
        Returns:
            StreamingQuery object
        """
        print(f"\n⚡ Writing streaming {dataset_name} data to {self.output_type}...")
        
        # Auto-generate checkpoint location if not provided
        if not checkpoint_location:
            checkpoint_location = f"./checkpoints/cleaned/{dataset_name}"
        
        if self.output_type == "json":
            return self._write_streaming_to_json(df, dataset_name, checkpoint_location, folder="cleaned")
        elif self.output_type == "minio":
            return self._write_streaming_to_minio(df, dataset_name, checkpoint_location, folder="cleaned")
        else:
            raise ValueError(f"Unknown output type: {self.output_type}")
    
    def write_enriched_data_streaming(self, df: DataFrame, dataset_name: str = "integrated", checkpoint_location: str = None):
        """
        Write streaming enriched/integrated data
        
        Args:
            df: Streaming DataFrame
            dataset_name: Name for output (default: "integrated")
            checkpoint_location: Path for checkpoint
            
        Returns:
            StreamingQuery object
        """
        print(f"\n⚡ Writing streaming final {dataset_name} data to {self.output_type}...")
        
        # Auto-generate checkpoint location if not provided
        if not checkpoint_location:
            checkpoint_location = f"./checkpoints/enriched/{dataset_name}"
        
        if self.output_type == "json":
            return self._write_streaming_to_json(df, f"{dataset_name}_final", checkpoint_location, folder="enriched")
        elif self.output_type == "minio":
            return self._write_streaming_to_minio(df, dataset_name, checkpoint_location, folder="enriched")
        else:
            raise ValueError(f"Unknown output type: {self.output_type}")
    
    def _write_streaming_to_json(self, df: DataFrame, dataset_name: str, checkpoint_location: str, folder: str = ""):
        """
        Write streaming data to JSON files
        
        Args:
            df: Streaming DataFrame
            dataset_name: Name of dataset
            checkpoint_location: Checkpoint path for fault tolerance
            folder: Subfolder in output directory
            
        Returns:
            StreamingQuery object
        """
        # Create output directory
        if folder:
            output_folder = os.path.join(self.output_dir, folder, dataset_name)
        else:
            output_folder = os.path.join(self.output_dir, dataset_name)
        
        os.makedirs(output_folder, exist_ok=True)
        os.makedirs(checkpoint_location, exist_ok=True)
        
        try:
            # Start streaming query
            query = df.writeStream \
                .outputMode("append") \
                .format("json") \
                .option("path", output_folder) \
                .option("checkpointLocation", checkpoint_location) \
                .start()
            
            print(f"   ✅ Streaming query started!")
            print(f"   📁 Output: {output_folder}")
            print(f"   🔖 Checkpoint: {checkpoint_location}")
            print(f"   📊 Query ID: {query.id}")
            print(f"   💡 Use query.status to monitor progress")
            
            return query
            
        except Exception as e:
            print(f"   ❌ Failed to start streaming query: {e}")
            raise
    
    def _write_streaming_to_minio(self, df: DataFrame, dataset_name: str, checkpoint_location: str, folder: str = "cleaned"):
        """
        Write streaming data to MinIO using Spark S3A
        
        Args:
            df: Streaming DataFrame
            dataset_name: Name of dataset
            checkpoint_location: Checkpoint path
            folder: Folder in bucket (cleaned, enriched, etc.)
            
        Returns:
            StreamingQuery object
        """
        if not self.minio_client:
            print(f"   ⚠️  MinIO client not initialized. Falling back to JSON.")
            return self._write_streaming_to_json(df, dataset_name, checkpoint_location, folder)
        
        from minio_config import MINIO_BUCKET
        
        # S3A path for streaming write
        s3_path = f"s3a://{MINIO_BUCKET}/{folder}/{dataset_name}"
        
        os.makedirs(checkpoint_location, exist_ok=True)
        
        try:
            # Start streaming query to MinIO via S3A
            query = df.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", s3_path) \
                .option("checkpointLocation", checkpoint_location) \
                .start()
            
            print(f"   ✅ Streaming query to MinIO started!")
            print(f"   📦 Bucket: {MINIO_BUCKET}")
            print(f"   📁 Path: {folder}/{dataset_name}")
            print(f"   🔖 Checkpoint: {checkpoint_location}")
            print(f"   📊 Query ID: {query.id}")
            print(f"   🔗 S3 URI: {s3_path}")
            
            return query
            
        except Exception as e:
            print(f"   ❌ Streaming to MinIO failed: {e}")
            print(f"   💡 Falling back to JSON streaming")
            return self._write_streaming_to_json(df, dataset_name, checkpoint_location, folder)


    def _write_to_hdfs(self, df: DataFrame, dataset_name: str):
        """
        Write to HDFS (for distributed storage)
        TODO: Implement when HDFS is available
        """
        print(f"   ⚠️  HDFS writer not implemented yet!")
        print(f"   📝 Will write to: hdfs://namenode:9000/data/{dataset_name}")
        print(f"   🔧 Configuration needed:")
        print(f"      - HDFS namenode URL")
        print(f"      - Authentication credentials")
        print(f"      - Output format (parquet recommended)")
        
        # Example implementation (commented out):
        """
        # Write as Parquet for better compression and performance
        hdfs_path = f"hdfs://namenode:9000/data/{dataset_name}"
        df.write \\
            .mode("overwrite") \\
            .format("parquet") \\
            .partitionBy("date")  # Partition by date for efficient queries \\
            .save(hdfs_path)
        
        print(f"   ✅ Saved {df.count()} records to HDFS: {hdfs_path}")
        """
        
        # For now, show sample data
        print(f"\n   📊 Sample data (what would be written to HDFS):")
        df.show(5, truncate=False)
        print(f"   📈 Total records: {df.count()}")
        
    def _write_to_elasticsearch(self, df: DataFrame, dataset_name: str):
        """
        Write to Elasticsearch (for search and analytics)
        TODO: Implement when Elasticsearch is available
        """
        index_name = f"weather-disaster-{dataset_name}"
        
        print(f"   ⚠️  Elasticsearch writer not implemented yet!")
        print(f"   📝 Will write to index: {index_name}")
        print(f"   🔧 Configuration needed:")
        print(f"      - Elasticsearch host and port")
        print(f"      - Authentication (username/password or API key)")
        print(f"      - Index mapping and settings")
        
        # Example implementation (commented out):
        """
        # Write to Elasticsearch
        df.write \\
            .format("org.elasticsearch.spark.sql") \\
            .option("es.nodes", "localhost") \\
            .option("es.port", "9200") \\
            .option("es.resource", index_name) \\
            .option("es.mapping.id", "id")  # Use 'id' field as document ID \\
            .option("es.write.operation", "upsert")  # Update or insert \\
            .mode("append") \\
            .save()
        
        print(f"   ✅ Indexed {df.count()} documents to ES: {index_name}")
        """
        
        # For now, show sample data
        print(f"\n   📊 Sample documents (what would be indexed to ES):")
        df.show(5, truncate=False)
        print(f"   📈 Total documents: {df.count()}")
        print(f"   💡 Index name: {index_name}")
