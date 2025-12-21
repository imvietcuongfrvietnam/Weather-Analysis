"""
Streaming Helper Functions
Support functions for both batch and streaming modes
"""

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def is_streaming(df: DataFrame) -> bool:
    """
    Check if DataFrame is a streaming DataFrame
    
    Args:
        df: DataFrame to check
        
    Returns:
        bool: True if streaming, False if batch
    """
    return df.isStreaming


def show_data_smart(df: DataFrame, name: str = "Data", num_rows: int = 5):
    """
    Show data - works for both batch and streaming
    
    Args:
        df: DataFrame to show
        name: Name to display
        num_rows: Number of rows (batch mode only)
    """
    print(f"\n📊 {name}:")
    
    if is_streaming(df):
        print(f"   ⚡ Streaming DataFrame - cannot show() directly")
        print(f"   📝 Will be displayed in streaming query output")
        print(f"   📋 Schema:")
        df.printSchema()
    else:
        print(f"   📦 Batch DataFrame")
        df.show(num_rows, truncate=False)


def count_data_smart(df: DataFrame, name: str = "Data") -> str:
    """
    Count data - works for both batch and streaming
    
    Args:
        df: DataFrame to count
        name: Name for display
        
    Returns:
        str: Count as string (for streaming, returns "N/A")
    """
    if is_streaming(df):
        return "N/A (streaming)"
    else:
        count = df.count()
        return f"{count:,}"


def write_streaming_to_console(df: DataFrame, query_name: str, num_rows: int = 10) -> StreamingQuery:
    """
    Write streaming DataFrame to console for debugging
    
    Args:
        df: Streaming DataFrame
        query_name: Name for the query
        num_rows: Number of rows to show
        
    Returns:
        StreamingQuery: The running query
    """
    query = df.writeStream \
        .queryName(query_name) \
        .format("console") \
        .option("numRows", num_rows) \
        .option("truncate", False) \
        .outputMode("append") \
        .start()
    
    return query


def write_to_minio_smart(df: DataFrame, data_writer, dataset_name: str, folder: str = "cleaned"):
    """
    Write to MinIO - works for both batch and streaming
    
    Args:
        df: DataFrame to write
        data_writer: DataWriter instance
        dataset_name: Name of dataset
        folder: Folder in bucket
    """
    if is_streaming(df):
        print(f"\n💾 Writing STREAMING data: {dataset_name} to MinIO ({folder})...")
        print(f"   ⚡ Using writeStream with checkpointing")
        
        # For streaming, we need to use writeStream
        # This is handled by a separate function
        # For now, just inform user
        print(f"   ⚠️  Streaming write requires checkpoint - see write_streaming_to_minio()")
    else:
        print(f"\n💾 Writing BATCH data: {dataset_name} to MinIO ({folder})...")
        data_writer.write_cleaned_data(df, dataset_name) if folder == "cleaned" else data_writer.write_enriched_data(df, dataset_name)


def stop_all_streaming_queries(spark):
    """
    Stop all active streaming queries
    
    Args:
        spark: SparkSession
    """
    active_queries = spark.streams.active
    if active_queries:
        print(f"\n🛑 Stopping {len(active_queries)} streaming queries...")
        for query in active_queries:
            print(f"   Stopping query: {query.name}")
            query.stop()
        print("   ✅ All queries stopped")
    else:
        print("\n✅ No active streaming queries to stop")
