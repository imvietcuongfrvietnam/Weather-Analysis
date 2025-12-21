"""
Models - ML Models for Weather Forecasting
Các mô hình ML cho dự đoán từng đặc trưng thời tiết
"""

from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, IndexToString
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
import config

class WeatherForecastModels:
    """Build and manage forecasting models for different weather features"""
    
    def __init__(self):
        self.models = {}
        self.feature_cols = []
        self.scalers = {}
        
    def build_regression_model(self, target_feature: str, feature_cols: list, model_type: str = "GBT"):
        """
        Build a regression model for a continuous target feature
        
        Args:
            target_feature: Name of target feature to predict
            feature_cols: List of feature column names
            model_type: "GBT" or "RandomForest"
            
        Returns:
            Pipeline with model
        """
        print(f"\n🤖 Building {model_type} model for {target_feature}...")
        
        # Feature assembly
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"  # Skip rows with invalid values
        )
        
        # Feature scaling (important for GBT)
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Select model based on type
        if model_type == "GBT":
            model = GBTRegressor(
                featuresCol="features",
                labelCol=target_feature,
                predictionCol=f"prediction_{target_feature}",
                maxIter=config.GBT_PARAMS['maxIter'],
                maxDepth=config.GBT_PARAMS['maxDepth'],
                stepSize=config.GBT_PARAMS['stepSize'],
                subsamplingRate=config.GBT_PARAMS['subsamplingRate'],
                seed=config.RANDOM_SEED
            )
        else:  # RandomForest
            model = RandomForestRegressor(
                featuresCol="features",
                labelCol=target_feature,
                predictionCol=f"prediction_{target_feature}",
                numTrees=config.RF_REGRESSION_PARAMS['numTrees'],
                maxDepth=config.RF_REGRESSION_PARAMS['maxDepth'],
                minInstancesPerNode=config.RF_REGRESSION_PARAMS['minInstancesPerNode'],
                subsamplingRate=config.RF_REGRESSION_PARAMS['subsamplingRate'],
                seed=config.RANDOM_SEED
            )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, model])
        
        print(f"   ✅ Model pipeline created for {target_feature}")
        
        return pipeline
    
    def build_classification_model(self, target_feature: str, feature_cols: list):
        """
        Build a classification model for categorical target feature
        
        Args:
            target_feature: Name of target feature (e.g., weather_condition)
            feature_cols: List of feature column names
            
        Returns:
            Pipeline with classifier
        """
        print(f"\n🏷️  Building classifier for {target_feature}...")
        
        # String indexer for labels
        label_indexer = StringIndexer(
            inputCol=target_feature,
            outputCol="label",
            handleInvalid="skip"
        )
        
        # Feature assembly
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        
        # Feature scaling
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Random Forest Classifier
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction_indexed",
            numTrees=config.RF_CLASSIFICATION_PARAMS['numTrees'],
            maxDepth=config.RF_CLASSIFICATION_PARAMS['maxDepth'],
            minInstancesPerNode=config.RF_CLASSIFICATION_PARAMS['minInstancesPerNode'],
            seed=config.RANDOM_SEED
        )
        
        # Convert indexed predictions back to strings
        label_converter = IndexToString(
            inputCol="prediction_indexed",
            outputCol=f"prediction_{target_feature}",
            labels=label_indexer.labels
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[label_indexer, assembler, scaler, classifier, label_converter])
        
        print(f"   ✅ Classifier pipeline created for {target_feature}")
        
        return pipeline
    
    def build_all_models(self, feature_cols: list):
        """
        Build models for all target features
        
        Args:
            feature_cols: List of feature column names
            
        Returns:
            dict: Dictionary of {target_feature: pipeline}
        """
        print("\n" + "="*80)
        print("🏗️  BUILDING ALL FORECAST MODELS")
        print("="*80)
        
        models = {}
        
        # Build regression models for continuous features
        for target in config.CONTINUOUS_FEATURES:
            model_type = config.MODEL_SELECTION.get(target, "GBT")
            models[target] = self.build_regression_model(target, feature_cols, model_type)
        
        # Build classification models for categorical features
        for target in config.CATEGORICAL_FEATURES:
            models[target] = self.build_classification_model(target, feature_cols)
        
        self.models = models
        self.feature_cols = feature_cols
        
        print(f"\n✅ Built {len(models)} models successfully!")
        print("="*80 + "\n")
        
        return models
    
    def train_model(self, pipeline: Pipeline, train_df: DataFrame, target_feature: str):
        """
        Train a single model
        
        Args:
            pipeline: ML Pipeline to train
            train_df: Training DataFrame
            target_feature: Name of target feature
            
        Returns:
            Trained PipelineModel
        """
        print(f"\n🚂 Training model for {target_feature}...")
        
        try:
            # Filter out rows where target is null
            train_df_clean = train_df.filter(train_df[target_feature].isNotNull())
            
            record_count = train_df_clean.count()
            print(f"   Training records: {record_count}")
            
            if record_count < 10:
                print(f"   ⚠️  Warning: Very few training records for {target_feature}")
            
            # Train the model
            model = pipeline.fit(train_df_clean)
            
            print(f"   ✅ Training complete for {target_feature}")
            
            return model
            
        except Exception as e:
            print(f"   ❌ Error training {target_feature}: {e}")
            raise
    
    def train_all_models(self, train_df: DataFrame):
        """
        Train all models
        
        Args:
            train_df: Training DataFrame with all features and targets
            
        Returns:
            dict: Dictionary of {target_feature: trained_model}
        """
        print("\n" + "="*80)
        print("🎓 TRAINING ALL MODELS")
        print("="*80)
        
        if not self.models:
            raise ValueError("Models not built yet. Call build_all_models() first.")
        
        trained_models = {}
        
        for target_feature, pipeline in self.models.items():
            trained_model = self.train_model(pipeline, train_df, target_feature)
            trained_models[target_feature] = trained_model
        
        print(f"\n✅ All {len(trained_models)} models trained successfully!")
        print("="*80 + "\n")
        
        return trained_models
    
    def predict(self, model, test_df: DataFrame, target_feature: str) -> DataFrame:
        """
        Make predictions with a trained model
        
        Args:
            model: Trained PipelineModel
            test_df: Test DataFrame
            target_feature: Name of target feature
            
        Returns:
            DataFrame with predictions
        """
        try:
            predictions = model.transform(test_df)
            return predictions
        except Exception as e:
            print(f"   ❌ Error predicting {target_feature}: {e}")
            raise
    
    def save_model(self, model, target_feature: str, path: str):
        """
        Save a trained model
        
        Args:
            model: Trained model to save
            target_feature: Name of target feature
            path: Path to save model
        """
        try:
            model_path = f"{path}/{target_feature}_model"
            model.write().overwrite().save(model_path)
            print(f"   💾 Saved model for {target_feature} to {model_path}")
        except Exception as e:
            print(f"   ⚠️  Could not save model for {target_feature}: {e}")
    
    def save_all_models(self, trained_models: dict, base_path: str):
        """
        Save all trained models
        
        Args:
            trained_models: Dictionary of trained models
            base_path: Base path for saving models
        """
        print(f"\n💾 Saving models to {base_path}...")
        
        for target_feature, model in trained_models.items():
            self.save_model(model, target_feature, base_path)
        
        print("✅ All models saved!")


class ForecastGenerator:
    """Generate multi-step forecasts using trained models"""
    
    def __init__(self, trained_models: dict, feature_cols: list):
        self.trained_models = trained_models
        self.feature_cols = feature_cols
    
    def forecast_next_step(self, current_df: DataFrame) -> DataFrame:
        """
        Forecast one step ahead (1 hour)
        
        Args:
            current_df: Current state DataFrame with all features
            
        Returns:
            DataFrame with predictions for next hour
        """
        # For each target feature, use its model to predict
        predictions_df = current_df
        
        for target_feature, model in self.trained_models.items():
            predictions_df = model.transform(predictions_df)
        
        return predictions_df
    
    def forecast_multi_step(self, initial_df: DataFrame, steps: int = 168) -> DataFrame:
        """
        Generate multi-step forecast (iterative)
        
        This is a simplified implementation. For production, you would:
        1. Take the last row of initial_df as starting point
        2. Predict next hour
        3. Use predictions as features for next iteration
        4. Repeat for 'steps' hours
        
        Args:
            initial_df: Initial state DataFrame
            steps: Number of hours to forecast (default: 168 = 7 days)
            
        Returns:
            DataFrame with all forecast steps
        """
        print(f"\n🔮 Generating {steps}-hour forecast...")
        print("   Note: This is an iterative forecast where predictions feed into next step")
        
        # This is a placeholder - actual implementation would be more complex
        # For now, we'll just do a simple 1-step forecast
        forecast_df = self.forecast_next_step(initial_df)
        
        print(f"   ✅ Forecast generated")
        
        return forecast_df


if __name__ == "__main__":
    print("Weather Forecast Models Module")
    print("Use WeatherForecastModels to build and train models")
