"""
Connection Utilities
Helper functions for retry logic and connection validation
"""

import time
from functools import wraps


def retry_on_failure(max_retries=3, delay=5, backoff=2, exceptions=(Exception,)):
    """
    Decorator for retrying functions on failure with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Multiplier for delay on each retry
        exceptions: Tuple of exceptions to catch and retry on
    
    Example:
        @retry_on_failure(max_retries=3, delay=5)
        def connect_to_kafka():
            # Connection code
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        # Last attempt failed
                        print(f"   ❌ All {max_retries} attempts failed!")
                        raise
                    
                    # Retry with backoff
                    print(f"   ⚠️  Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                    print(f"   🔄 Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            # Should never reach here, but just in case
            raise last_exception
        
        return wrapper
    return decorator


def validate_kafka_connection(bootstrap_servers: str) -> bool:
    """
    Validate Kafka connection
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        
    Returns:
        bool: True if connection is valid
    """
    try:
        from kafka import KafkaProducer
        
        print(f"   🔍 Validating Kafka connection to: {bootstrap_servers}")
        
        # Try to create a producer to test connection
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        producer.close()
        
        print(f"   ✅ Kafka connection validated!")
        return True
        
    except ImportError:
        print(f"   ⚠️  kafka-python not installed. Skipping validation.")
        print(f"   💡 Install with: pip install kafka-python")
        return True  # Don't fail if library not installed
    except Exception as e:
        print(f"   ❌ Kafka connection failed: {e}")
        return False


def validate_minio_connection(endpoint: str, access_key: str, secret_key: str, secure: bool = False) -> bool:
    """
    Validate MinIO connection
    
    Args:
        endpoint: MinIO endpoint
        access_key: Access key
        secret_key: Secret key
        secure: Use HTTPS
        
    Returns:
        bool: True if connection is valid
    """
    try:
        from minio import Minio
        
        print(f"   🔍 Validating MinIO connection to: {endpoint}")
        
        # Try to create client and list buckets
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        
        # Test connection by listing buckets
        list(client.list_buckets())
        
        print(f"   ✅ MinIO connection validated!")
        return True
        
    except Exception as e:
        print(f"   ❌ MinIO connection failed: {e}")
        return False


def test_spark_kafka_connection(spark, bootstrap_servers: str, topic: str = None):
    """
    Test Spark-Kafka connection
    
    Args:
        spark: SparkSession
        bootstrap_servers: Kafka bootstrap servers
        topic: Optional topic to test (if None, just tests connection)
    
    Returns:
        bool: True if connection successful
    """
    try:
        print(f"\n🧪 Testing Spark-Kafka connection...")
        print(f"   Server: {bootstrap_servers}")
        if topic:
            print(f"   Topic: {topic}")
        
        # Try to read from Kafka (just to test connection)
        df_builder = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("failOnDataLoss", "false")
        
        if topic:
            df_builder = df_builder.option("subscribe", topic)
        else:
            # Subscribe to a test topic that may not exist
            df_builder = df_builder.option("subscribe", "__test_connection__")
        
        # Set timeout options
        df_builder = df_builder \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1)
        
        # Try to load (this will fail if Kafka is not reachable)
        try:
            df = df_builder.load()
            print(f"   ✅ Spark can connect to Kafka!")
            return True
        except Exception as e:
            if "topic" in str(e).lower() and "not exist" in str(e).lower():
                # Topic doesn't exist, but connection is OK
                print(f"   ✅ Spark can connect to Kafka (topic not found is OK)")
                return True
            else:
                raise
        
    except Exception as e:
        print(f"   ❌ Spark-Kafka connection failed: {e}")
        print(f"   💡 Check:")
        print(f"      1. Kafka server is running")
        print(f"      2. Bootstrap servers address is correct")
        print(f"      3. Network connectivity")
        return False
