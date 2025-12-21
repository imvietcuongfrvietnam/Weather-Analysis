"""
Forecast Evaluator - Metrics and Evaluation for Weather Forecasting
Đánh giá độ chính xác của các mô hình dự đoán
Updated: Optimized for Spark ML 3.x
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs, mean
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
import sys
import os

# Setup import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import config
except ImportError:
    # Fallback config
    class Config:
        CONTINUOUS_FEATURES = ["temperature", "humidity", "pressure", "wind_speed", "precipitation_mm"]
        CATEGORICAL_FEATURES = ["weather_desc"]
    config = Config()

class ForecastEvaluator:
    """Evaluate forecast model performance"""
    
    @staticmethod
    def evaluate_regression(predictions_df: DataFrame, target_feature: str) -> dict:
        """
        Đánh giá mô hình hồi quy (Regression)
        Metrics: MAE, RMSE, R2, MAPE
        """
        prediction_col = f"prediction_{target_feature}"
        
        # Kiểm tra cột tồn tại
        if prediction_col not in predictions_df.columns or target_feature not in predictions_df.columns:
            print(f"   ⚠️  Skipping {target_feature}: Prediction or Target column not found.")
            return {}
        
        # Lọc bỏ giá trị null để tránh lỗi tính toán
        eval_df = predictions_df.select(target_feature, prediction_col).dropna()
        
        count = eval_df.count()
        if count == 0:
            print(f"   ⚠️  Skipping {target_feature}: No valid rows to evaluate.")
            return {}

        metrics = {}
        
        # 1. RMSE (Root Mean Squared Error)
        rmse_evaluator = RegressionEvaluator(
            labelCol=target_feature, predictionCol=prediction_col, metricName="rmse"
        )
        metrics['rmse'] = rmse_evaluator.evaluate(eval_df)
        
        # 2. MAE (Mean Absolute Error)
        mae_evaluator = RegressionEvaluator(
            labelCol=target_feature, predictionCol=prediction_col, metricName="mae"
        )
        metrics['mae'] = mae_evaluator.evaluate(eval_df)
        
        # 3. R2 (R-squared)
        r2_evaluator = RegressionEvaluator(
            labelCol=target_feature, predictionCol=prediction_col, metricName="r2"
        )
        metrics['r2'] = r2_evaluator.evaluate(eval_df)
        
        # 4. MAPE (Mean Absolute Percentage Error) - Tính thủ công vì Spark cũ không có sẵn
        # MAPE = mean( abs((actual - pred) / actual) ) * 100
        # Thêm 0.001 vào mẫu số để tránh chia cho 0
        mape_df = eval_df.withColumn(
            "ape", 
            spark_abs((col(target_feature) - col(prediction_col)) / (spark_abs(col(target_feature)) + 0.001))
        )
        metrics['mape'] = mape_df.agg(mean("ape")).collect()[0][0] * 100
        
        metrics['sample_count'] = count
        
        return metrics
    
    @staticmethod
    def evaluate_classification(predictions_df: DataFrame, target_feature: str) -> dict:
        """
        Đánh giá mô hình phân loại (Classification)
        Metrics: Accuracy, F1, Precision, Recall
        """
        # Cột dự đoán (dạng số index)
        prediction_col = f"prediction_{target_feature}"
        # Cột label thực tế (dạng số index - do StringIndexer tạo ra)
        label_col = f"{target_feature}_index"
        
        if prediction_col not in predictions_df.columns or label_col not in predictions_df.columns:
            print(f"   ⚠️  Skipping {target_feature}: Columns not found ({label_col}, {prediction_col})")
            return {}
        
        eval_df = predictions_df.select(label_col, prediction_col).dropna()
        count = eval_df.count()
        
        if count == 0:
            return {}
            
        metrics = {}
        
        # Helper function để tạo evaluator
        def get_evaluator(metric_name):
            return MulticlassClassificationEvaluator(
                labelCol=label_col,
                predictionCol=prediction_col,
                metricName=metric_name
            )

        metrics['accuracy'] = get_evaluator("accuracy").evaluate(eval_df)
        metrics['f1'] = get_evaluator("f1").evaluate(eval_df)
        metrics['precision'] = get_evaluator("weightedPrecision").evaluate(eval_df)
        metrics['recall'] = get_evaluator("weightedRecall").evaluate(eval_df)
        metrics['sample_count'] = count
        
        return metrics
    
    @staticmethod
    def evaluate_all_models(predictions_df: DataFrame) -> dict:
        """
        Đánh giá toàn bộ các mô hình (cả Regression và Classification)
        """
        print("\n" + "="*60)
        print("📊 EVALUATING MODEL PERFORMANCE")
        print("="*60)
        
        all_metrics = {}
        
        # 1. Evaluate Regression Models
        print("\n🔢 Regression Models:")
        for target in config.CONTINUOUS_FEATURES:
            # Chỉ đánh giá nếu có cột dự đoán trong DataFrame
            if f"prediction_{target}" in predictions_df.columns:
                print(f"   Evaluating {target}...")
                metrics = ForecastEvaluator.evaluate_regression(predictions_df, target)
                if metrics:
                    all_metrics[target] = metrics
                    print(f"      RMSE: {metrics['rmse']:.4f}, R2: {metrics['r2']:.4f}")
        
        # 2. Evaluate Classification Models
        # (Nếu bạn chưa implement classification thì phần này sẽ skip)
        if hasattr(config, 'CATEGORICAL_FEATURES'):
            print("\n🏷️  Classification Models:")
            for target in config.CATEGORICAL_FEATURES:
                if f"prediction_{target}" in predictions_df.columns:
                    print(f"   Evaluating {target}...")
                    metrics = ForecastEvaluator.evaluate_classification(predictions_df, target)
                    if metrics:
                        all_metrics[target] = metrics
                        print(f"      Accuracy: {metrics['accuracy']:.4f}, F1: {metrics['f1']:.4f}")
        
        print("="*60 + "\n")
        return all_metrics

if __name__ == "__main__":
    print("Forecast Evaluator Module Loaded.")