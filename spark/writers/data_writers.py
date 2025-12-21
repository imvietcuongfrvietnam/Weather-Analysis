"""
FAKE Data Writers
In ra console hoặc fake file cho testing
Sau này thay bằng real HDFS/Elasticsearch writers
"""

from pyspark.sql import DataFrame

class FakeDataWriter:
    """
    Fake writer để test
    Chỉ in ra màn hình hoặc save local CSV
    """
    
    @staticmethod
    def write_to_console(df: DataFrame, name="data", num_rows=20):
        """
        FAKE: In ra console
        REAL: Sẽ write to HDFS/ES
        """
        print(f"\n{'='*60}")
        print(f"💾 [FAKE] Writing {name} to CONSOLE")
        print(f"{'='*60}")
        
        print(f"\n📊 Schema:")
        df.printSchema()
        
        print(f"\n📋 Sample Data ({num_rows} rows):")
        df.show(num_rows, truncate=False)
        
        print(f"\n📈 Statistics:")
        print(f"   Total rows: {df.count()}")
        print(f"   Total columns: {len(df.columns)}")
        
        print(f"\n✅ [FAKE] Data written to console")
    
    @staticmethod
    def write_to_fake_hdfs(df: DataFrame, path, format="parquet", mode="overwrite"):
        """
        FAKE: Save to local file (giả HDFS)
        REAL: Sẽ write to HDFS
        """
        print(f"\n💾 [FAKE] Writing to fake HDFS: {path}")
        print(f"   Format: {format}")
        print(f"   Mode: {mode}")
        print(f"   Rows: {df.count()}")
        
        # Save to local temp directory
        local_path = f"./fake_output/{path}"
        
        try:
            if format == "parquet":
                df.write.mode(mode).parquet(local_path)
            elif format == "csv":
                df.write.mode(mode).option("header", True).csv(local_path)
            elif format == "json":
                df.write.mode(mode).json(local_path)
            
            print(f"   ✅ [FAKE] Saved to local: {local_path}")
        except Exception as e:
            print(f"   ⚠️  Could not save file: {e}")
            print(f"   📊 Showing data instead:")
            df.show(10)
    
    @staticmethod
    def write_to_fake_elasticsearch(df: DataFrame, index="weather-data"):
        """
        FAKE: Giả lập write to Elasticsearch
        REAL: Sẽ write to ES
        """
        print(f"\n🔍 [FAKE] Indexing to Elasticsearch")
        print(f"   Index: {index}")
        print(f"   Documents: {df.count()}")
        
        # Show first few records as JSON
        print(f"\n📄 Sample documents:")
        df.select("*").limit(5).show(5, truncate=False)
        
        print(f"   ✅ [FAKE] Would be indexed to ES: {index}")


# ========================================
# REAL DATA WRITERS (Template for future)
# ========================================

class RealDataWriter:
    """
    Template cho real writers
    Sẽ implement sau khi có HDFS/ES
    """
    
    @staticmethod
    def write_to_hdfs(df: DataFrame, path, format="parquet", mode="overwrite", partition_by=None):
        """
        TODO: Real HDFS writer
        """
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        if format == "parquet":
            writer.parquet(path)
        elif format == "orc":
            writer.orc(path)
        elif format == "csv":
            writer.option("header", True).csv(path)
        
        print(f"✅ Written to HDFS: {path}")
    
    @staticmethod
    def write_to_elasticsearch(df: DataFrame, index, es_nodes="localhost", es_port="9200"):
        """
        TODO: Real Elasticsearch writer
        """
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", es_nodes) \
            .option("es.port", es_port) \
            .option("es.resource", index) \
            .option("es.batch.write.retry.count", "3") \
            .save()
        
        print(f"✅ Indexed to Elasticsearch: {index}")
