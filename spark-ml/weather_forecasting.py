"""
Weather Forecasting - Main Pipeline
Dự đoán thời tiết 7 ngày sử dụng Spark ML và dữ liệu từ MinIO

Hệ thống dự đoán các đặc trưng:
- Nhiệt độ (°C, °F)
- Độ ẩm (%)
- Áp suất khí quyển (hPa)
- Tốc độ gió (km/h)
- Lượng mưa (mm)
- Tình trạng thời tiết (phân loại)
"""

from pyspark.sql import SparkSession
import sys
import os
import argparse
from datetime import datetime

# Import modules
import config
from data_loader import WeatherDataLoader
from feature_engineering import TimeSeriesFeatureEngineer
from models import WeatherForecastModels
from forecast_evaluator import ForecastEvaluator
from visualization import ForecastVisualizer
from postgres_writer import PostgresWriter

def create_spark_session():
    """
    Create Spark session with MinIO S3A configuration
    
    Returns:
        SparkSession configured for MinIO access
    """
    print("\n" + "="*80)
    print("🚀 WEATHER FORECASTING ML SYSTEM")
    print("="*80)
    print("⚡ Initializing Spark Session...")
    
    # S3A/Hadoop packages for MinIO + PostgreSQL JDBC
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.6.0"  # PostgreSQL JDBC driver
    ]
    
    builder = SparkSession.builder \
        .appName("Weather7DayForecast") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars.ivy", "/tmp/.ivy2")
    
    # Add MinIO S3A configurations
    for key, value in config.SPARK_S3A_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark Session initialized successfully!")
    
    return spark


def run_forecasting_pipeline(city: str = None, limit_rows: int = None, 
                             save_models: bool = True, create_plots: bool = True):
    """
    Run the complete forecasting pipeline
    
    Args:
        city: Filter data by city (optional)
        limit_rows: Limit number of rows for testing (optional)
        save_models: Whether to save trained models
        create_plots: Whether to create visualization plots
    """
    
    # Print configuration
    config.print_config()
    
    # 1. Create Spark Session
    spark = create_spark_session()
    
    try:
        # ==========================================
        # STEP 1: LOAD DATA FROM MINIO
        # ==========================================
        print("\n" + "="*80)
        print("STEP 1: LOADING DATA FROM MINIO")
        print("="*80)
        
        loader = WeatherDataLoader(spark)
        df = loader.load_data(city=city, limit_rows=limit_rows)
        
        # Validate data quality
        validation = loader.validate_data(df)
        
        if validation['quality_score'] < 50:
            print("\n⚠️  WARNING: Data quality is poor. Results may not be reliable.")
            response = input("Continue anyway? (y/n): ")
            if response.lower() != 'y':
                print("Exiting...")
                return
        
        # Show summary
        loader.summary_stats(df)
        
        # ==========================================
        # STEP 2: FEATURE ENGINEERING
        # ==========================================
        print("\n" + "="*80)
        print("STEP 2: FEATURE ENGINEERING")
        print("="*80)
        
        engineer = TimeSeriesFeatureEngineer()
        df_features = engineer.engineer_all_features(df)
        
        # Get feature columns (excluding targets and metadata)
        feature_cols = engineer.get_feature_columns(df_features, exclude_targets=True)
        
        print(f"\n📊 Total features created: {len(feature_cols)}")
        print(f"   Features: {feature_cols[:10]}...")  # Show first 10
        
        # ==========================================
        # STEP 3: TRAIN/TEST SPLIT
        # ==========================================
        print("\n" + "="*80)
        print("STEP 3: SPLITTING DATA")
        print("="*80)
        
        # Remove rows with any null values in target features
        # (This is important for training)
        df_clean = df_features
        for target in config.ALL_TARGET_FEATURES:
            if target in df_clean.columns:
                df_clean = df_clean.filter(df_clean[target].isNotNull())
        
        total_rows = df_clean.count()
        print(f"Clean dataset: {total_rows} rows")
        
        if total_rows < config.MIN_TRAINING_RECORDS:
            print(f"\n❌ Error: Insufficient data ({total_rows} rows)")
            print(f"   Minimum required: {config.MIN_TRAINING_RECORDS} rows")
            return
        
        # Split into train/test
        train_df, test_df = df_clean.randomSplit(
            [config.TRAIN_TEST_SPLIT, 1 - config.TRAIN_TEST_SPLIT],
            seed=config.RANDOM_SEED
        )
        
        train_count = train_df.count()
        test_count = test_df.count()
        
        print(f"Training set:   {train_count} rows ({config.TRAIN_TEST_SPLIT*100:.0f}%)")
        print(f"Test set:       {test_count} rows ({(1-config.TRAIN_TEST_SPLIT)*100:.0f}%)")
        
        # ==========================================
        # STEP 4: BUILD MODELS
        # ==========================================
        print("\n" + "="*80)
        print("STEP 4: BUILDING MODELS")
        print("="*80)
        
        model_builder = WeatherForecastModels()
        pipelines = model_builder.build_all_models(feature_cols)
        
        # ==========================================
        # STEP 5: TRAIN MODELS
        # ==========================================
        print("\n" + "="*80)
        print("STEP 5: TRAINING MODELS")
        print("="*80)
        
        trained_models = model_builder.train_all_models(train_df)
        
        # Save models if requested
        if save_models:
            print(f"\n💾 Saving models to {config.LOCAL_MODEL_DIR}...")
            model_builder.save_all_models(trained_models, config.LOCAL_MODEL_DIR)
        
        # ==========================================
        # STEP 6: MAKE PREDICTIONS
        # ==========================================
        print("\n" + "="*80)
        print("STEP 6: MAKING PREDICTIONS")
        print("="*80)
        
        # Predict on test set
        predictions_df = test_df
        
        for target_feature, model in trained_models.items():
            print(f"   Predicting {target_feature}...")
            predictions_df = model.transform(predictions_df)
        
        print(f"\n✅ Predictions complete for all {len(trained_models)} features")
        
        # ==========================================
        # STEP 7: EVALUATE MODELS
        # ==========================================
        print("\n" + "="*80)
        print("STEP 7: EVALUATING MODELS")
        print("="*80)
        
        evaluator = ForecastEvaluator()
        all_metrics = evaluator.evaluate_all_models(predictions_df)
        
        # Print summary
        evaluator.print_performance_summary(all_metrics)
        
        # Get best/worst features
        performance = evaluator.get_best_worst_features(all_metrics)
        
        if performance['best_regression']:
            print(f"\n🏆 Best regression model: {performance['best_regression']} "
                  f"(R² = {performance['regression_scores'][performance['best_regression']]:.4f})")
        
        if performance['worst_regression']:
            print(f"⚠️  Worst regression model: {performance['worst_regression']} "
                  f"(R² = {performance['regression_scores'][performance['worst_regression']]:.4f})")
        
        # ==========================================
        # STEP 8: EXPORT PREDICTIONS
        # ==========================================
        print("\n" + "="*80)
        print("STEP 8: EXPORTING PREDICTIONS")
        print("="*80)
        
        # Select relevant columns for export
        export_cols = ['datetime', 'city'] + config.ALL_TARGET_FEATURES
        
        # Add prediction columns
        for target in config.ALL_TARGET_FEATURES:
            pred_col = f"prediction_{target}"
            if pred_col in predictions_df.columns:
                export_cols.append(pred_col)
        
        export_df = predictions_df.select(export_cols)
        
        # Convert to Pandas for easier export
        print("   Converting to Pandas DataFrame...")
        export_pandas = export_df.toPandas()
        
        # Save to CSV
        output_file = os.path.join(config.LOCAL_OUTPUT_DIR, 
                                   f"weather_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        export_pandas.to_csv(output_file, index=False)
        print(f"   💾 Saved predictions to: {output_file}")
        
        # Show sample
        print("\n📊 Sample predictions:")
        print(export_pandas.head(10))
        
        # ==========================================
        # STEP 9: WRITE TO POSTGRESQL (OPTIONAL)
        # ==========================================
        print("\n" + "="*80)
        print("STEP 9: WRITING TO POSTGRESQL")
        print("="*80)
        
        postgres_writer = PostgresWriter()
        
        # Try to write to PostgreSQL (safe - won't fail if server not available)
        postgres_success = postgres_writer.write_predictions_safe(export_df)
        
        if not postgres_success:
            print("\n💡 To enable PostgreSQL export:")
            print("   1. Set up PostgreSQL server")
            print("   2. Run: python postgres_writer.py")
            print("   3. Follow setup instructions")
        
        # ==========================================
        # STEP 10: CREATE VISUALIZATIONS
        # ==========================================
        if create_plots:
            print("\n" + "="*80)
            print("STEP 10: CREATING VISUALIZATIONS")
            print("="*80)
            
            visualizer = ForecastVisualizer()
            
            # Plot all features
            visualizer.plot_all_features(export_pandas)
            
            # Create dashboard
            visualizer.plot_multi_panel_dashboard(export_pandas)
            
            # Plot metrics comparison
            visualizer.plot_metrics_comparison(all_metrics)
            
            print(f"\n✅ All visualizations saved to: {config.LOCAL_PLOTS_DIR}")
        
        # ==========================================
        # FINAL SUMMARY
        # ==========================================
        print("\n" + "="*80)
        print("✅ FORECASTING PIPELINE COMPLETE!")
        print("="*80)
        print(f"\n📁 Outputs:")
        print(f"   Predictions:  {output_file}")
        print(f"   Models:       {config.LOCAL_MODEL_DIR}/")
        if create_plots:
            print(f"   Plots:        {config.LOCAL_PLOTS_DIR}/")
        print("\n" + "="*80 + "\n")
        
        return {
            'predictions': export_pandas,
            'metrics': all_metrics,
            'models': trained_models,
            'validation': validation
        }
        
    except Exception as e:
        print(f"\n❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        # Stop Spark session
        print("\n🛑 Stopping Spark session...")
        spark.stop()


def main():
    """Main entry point with command-line arguments"""
    
    parser = argparse.ArgumentParser(description='Weather Forecasting ML System')
    
    parser.add_argument('--city', type=str, default=None,
                       help='Filter data by city name')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of rows for testing')
    parser.add_argument('--no-save-models', action='store_true',
                       help='Do not save trained models')
    parser.add_argument('--no-plots', action='store_true',
                       help='Do not create visualization plots')
    
    args = parser.parse_args()
    
    # Run pipeline
    results = run_forecasting_pipeline(
        city=args.city,
        limit_rows=args.limit,
        save_models=not args.no_save_models,
        create_plots=not args.no_plots
    )
    
    return results


if __name__ == "__main__":
    main()
