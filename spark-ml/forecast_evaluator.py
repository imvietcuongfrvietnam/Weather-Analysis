"""
Forecast Evaluator - Metrics and Evaluation for Weather Forecasting
Đánh giá độ chính xác của các mô hình dự đoán
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs, pow as spark_pow, mean, sqrt, count, when
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
import config

class ForecastEvaluator:
    """Evaluate forecast model performance"""
    
    @staticmethod
    def evaluate_regression(predictions_df: DataFrame, target_feature: str) -> dict:
        """
        Evaluate regression model performance
        
        Args:
            predictions_df: DataFrame with predictions and actual values
            target_feature: Name of target feature
            
        Returns:
            dict with evaluation metrics
        """
        prediction_col = f"prediction_{target_feature}"
        
        if prediction_col not in predictions_df.columns:
            print(f"   ⚠️  No predictions found for {target_feature}")
            return {}
        
        # Filter out nulls
        eval_df = predictions_df.filter(
            col(target_feature).isNotNull() & col(prediction_col).isNotNull()
        )
        
        # Calculate metrics using Spark evaluators
        metrics = {}
        
        # MAE (Mean Absolute Error)
        mae_evaluator = RegressionEvaluator(
            labelCol=target_feature,
            predictionCol=prediction_col,
            metricName="mae"
        )
        metrics['MAE'] = mae_evaluator.evaluate(eval_df)
        
        # RMSE (Root Mean Squared Error)
        rmse_evaluator = RegressionEvaluator(
            labelCol=target_feature,
            predictionCol=prediction_col,
            metricName="rmse"
        )
        metrics['RMSE'] = rmse_evaluator.evaluate(eval_df)
        
        # R² Score
        r2_evaluator = RegressionEvaluator(
            labelCol=target_feature,
            predictionCol=prediction_col,
            metricName="r2"
        )
        metrics['R2'] = r2_evaluator.evaluate(eval_df)
        
        # MAPE (Mean Absolute Percentage Error) - calculated manually
        mape_df = eval_df.withColumn(
            "ape",
            spark_abs(col(target_feature) - col(prediction_col)) / 
            (spark_abs(col(target_feature)) + 0.001)  # Add small value to avoid division by zero
        )
        metrics['MAPE'] = mape_df.agg(mean("ape")).collect()[0][0] * 100  # Convert to percentage
        
        # Sample count
        metrics['sample_count'] = eval_df.count()
        
        return metrics
    
    @staticmethod
    def evaluate_classification(predictions_df: DataFrame, target_feature: str) -> dict:
        """
        Evaluate classification model performance
        
        Args:
            predictions_df: DataFrame with predictions and actual values
            target_feature: Name of target feature
            
        Returns:
            dict with evaluation metrics
        """
        prediction_col = f"prediction_{target_feature}"
        
        if prediction_col not in predictions_df.columns:
            print(f"   ⚠️  No predictions found for {target_feature}")
            return {}
        
        # For classification, we need indexed labels
        # The model pipeline already created these
        
        # Filter out nulls
        eval_df = predictions_df.filter(
            col(target_feature).isNotNull() & col(prediction_col).isNotNull()
        )
        
        metrics = {}
        
        # Accuracy
        accuracy_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction_indexed",
            metricName="accuracy"
        )
        metrics['Accuracy'] = accuracy_evaluator.evaluate(eval_df)
        
        # F1 Score  
        f1_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction_indexed",
            metricName="f1"
        )
        metrics['F1'] = f1_evaluator.evaluate(eval_df)
        
        # Weighted Precision
        precision_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction_indexed",
            metricName="weightedPrecision"
        )
        metrics['Precision'] = precision_evaluator.evaluate(eval_df)
        
        # Weighted Recall
        recall_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction_indexed",
            metricName="weightedRecall"
        )
        metrics['Recall'] = recall_evaluator.evaluate(eval_df)
        
        # Sample count
        metrics['sample_count'] = eval_df.count()
        
        return metrics
    
    @staticmethod
    def evaluate_all_models(predictions_df: DataFrame) -> dict:
        """
        Evaluate all models
        
        Args:
            predictions_df: DataFrame with predictions for all features
            
        Returns:
            dict: {target_feature: metrics_dict}
        """
        print("\n" + "="*80)
        print("📊 EVALUATING MODEL PERFORMANCE")
        print("="*80)
        
        all_metrics = {}
        
        # Evaluate regression models
        print("\n🔢 Regression Models:")
        for target in config.CONTINUOUS_FEATURES:
            print(f"\n  {target}:")
            metrics = ForecastEvaluator.evaluate_regression(predictions_df, target)
            all_metrics[target] = metrics
            
            if metrics:
                print(f"    MAE:   {metrics['MAE']:.4f}")
                print(f"    RMSE:  {metrics['RMSE']:.4f}")
                print(f"    R²:    {metrics['R2']:.4f}")
                print(f"    MAPE:  {metrics['MAPE']:.2f}%")
                print(f"    Samples: {metrics['sample_count']}")
        
        # Evaluate classification models
        print("\n\n🏷️  Classification Models:")
        for target in config.CATEGORICAL_FEATURES:
            print(f"\n  {target}:")
            metrics = ForecastEvaluator.evaluate_classification(predictions_df, target)
            all_metrics[target] = metrics
            
            if metrics:
                print(f"    Accuracy:  {metrics['Accuracy']:.4f}")
                print(f"    F1:        {metrics['F1']:.4f}")
                print(f"    Precision: {metrics['Precision']:.4f}")
                print(f"    Recall:    {metrics['Recall']:.4f}")
                print(f"    Samples:   {metrics['sample_count']}")
        
        print("\n" + "="*80 + "\n")
        
        return all_metrics
    
    @staticmethod
    def print_performance_summary(all_metrics: dict):
        """
        Print a summary table of model performance
        
        Args:
            all_metrics: Dictionary of metrics from evaluate_all_models()
        """
        print("\n" + "="*80)
        print("📈 MODEL PERFORMANCE SUMMARY")
        print("="*80)
        
        # Regression models
        print("\n🔢 Continuous Features (Regression):")
        print(f"{'Feature':<20} {'MAE':<10} {'RMSE':<10} {'R²':<10} {'MAPE':<10}")
        print("-" * 60)
        
        for target in config.CONTINUOUS_FEATURES:
            if target in all_metrics and all_metrics[target]:
                m = all_metrics[target]
                print(f"{target:<20} {m['MAE']:<10.4f} {m['RMSE']:<10.4f} "
                      f"{m['R2']:<10.4f} {m['MAPE']:<10.2f}%")
        
        # Classification models
        print("\n🏷️  Categorical Features (Classification):")
        print(f"{'Feature':<20} {'Accuracy':<12} {'F1':<10} {'Precision':<12} {'Recall':<10}")
        print("-" * 64)
        
        for target in config.CATEGORICAL_FEATURES:
            if target in all_metrics and all_metrics[target]:
                m = all_metrics[target]
                print(f"{target:<20} {m['Accuracy']:<12.4f} {m['F1']:<10.4f} "
                      f"{m['Precision']:<12.4f} {m['Recall']:<10.4f}")
        
        print("="*80 + "\n")
    
    @staticmethod
    def get_best_worst_features(all_metrics: dict) -> dict:
        """
        Identify best and worst performing features
        
        Args:
            all_metrics: Dictionary of metrics
            
        Returns:
            dict with 'best' and 'worst' feature info
        """
        # For regression, use R² score
        regression_scores = {}
        for target in config.CONTINUOUS_FEATURES:
            if target in all_metrics and 'R2' in all_metrics[target]:
                regression_scores[target] = all_metrics[target]['R2']
        
        if regression_scores:
            best_regression = max(regression_scores, key=regression_scores.get)
            worst_regression = min(regression_scores, key=regression_scores.get)
        else:
            best_regression = worst_regression = None
        
        # For classification, use F1 score
        classification_scores = {}
        for target in config.CATEGORICAL_FEATURES:
            if target in all_metrics and 'F1' in all_metrics[target]:
                classification_scores[target] = all_metrics[target]['F1']
        
        if classification_scores:
            best_classification = max(classification_scores, key=classification_scores.get)
            worst_classification = min(classification_scores, key=classification_scores.get)
        else:
            best_classification = worst_classification = None
        
        return {
            'best_regression': best_regression,
            'worst_regression': worst_regression,
            'best_classification': best_classification,
            'worst_classification': worst_classification,
            'regression_scores': regression_scores,
            'classification_scores': classification_scores
        }


if __name__ == "__main__":
    print("Forecast Evaluator Module")
    print("Use ForecastEvaluator to evaluate model performance")
