"""
Real Data Writers
Write data to JSON files (for testing), HDFS (for storage), or Elasticsearch (for indexing)
"""

from pyspark.sql import DataFrame
import os

class DataWriter:
    """
    Data writer with multiple destinations:
    - JSON files (for testing and viewing cleaned data)
    - HDFS (for distributed storage)
    - Elasticsearch (for search and analytics)
    """
    
    def __init__(self, output_type: str = "json"):
        """
        Args:
            output_type: "json", "hdfs", or "elasticsearch"
        """
        self.output_type = output_type
        self.output_dir = "./output"
        
    def write_cleaned_data(self, df: DataFrame, dataset_name: str):
        """
        Write cleaned data to configured destination
        
        Args:
            df: DataFrame to write
            dataset_name: Name of dataset (e.g., "weather", "311", "taxi", "collision")
        """
        print(f"\n💾 Writing {dataset_name} data to {self.output_type}...")
        
        if self.output_type == "json":
            self._write_to_json(df, dataset_name)
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
            self._write_to_json(df, f"{dataset_name}_final")
        elif self.output_type == "hdfs":
            self._write_to_hdfs(df, f"{dataset_name}_final")
        elif self.output_type == "elasticsearch":
            self._write_to_elasticsearch(df, f"{dataset_name}_final")
        else:
            raise ValueError(f"Unknown output type: {self.output_type}")
    
    # ============================================
    # Private methods for different destinations
    # ============================================
    
    def _write_to_json(self, df: DataFrame, dataset_name: str):
        """
        Write to JSON files using pandas (Windows compatible, no Hadoop needed)
        This allows viewing cleaned data easily
        """
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, f"{dataset_name}_cleaned.json")
        
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
