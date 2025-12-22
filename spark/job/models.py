"""
Models - ML Models for Weather Forecasting
Các mô hình ML cho dự đoán từng đặc trưng thời tiết
Updated: Fix 'NoSuchElementException' in IndexToString by explicitly setting labels after training
"""

from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, IndexToString
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import DataFrame
import sys
import os

# --- IMPORT CONFIG ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    import config
except ImportError:
    class Config:
        CONTINUOUS_FEATURES = ["temperature", 
        "humidity", 
        "pressure", 
        "wind_speed", 
        "wind_direction"]
        CATEGORICAL_FEATURES = ["weather_desc"]
        GBT_PARAMS = {'maxIter': 20, 'maxDepth': 5, 'stepSize': 0.1, 'subsamplingRate': 0.8}
        RF_REGRESSION_PARAMS = {'numTrees': 20, 'maxDepth': 5, 'minInstancesPerNode': 2, 'subsamplingRate': 0.8}
        RF_CLASSIFICATION_PARAMS = {'numTrees': 20, 'maxDepth': 5, 'minInstancesPerNode': 2}
        MODEL_SELECTION = {}
        RANDOM_SEED = 42
    config = Config()

class WeatherForecastModels:
    """Build and manage forecasting models for different weather features"""
    
    def __init__(self):
        self.models = {}
        self.feature_cols = []
        
    def build_regression_model(self, target_feature: str, feature_cols: list, model_type: str = "GBT"):
        """
        Xây dựng Pipeline cho bài toán hồi quy (Dự đoán số)
        
        """
        print(f"\n🤖 Building {model_type} model for {target_feature}...")
        
        # Unique column names
        features_raw_col = f"features_raw_{target_feature}"
        features_scaled_col = f"features_{target_feature}"
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol=features_raw_col,
            handleInvalid="skip" 
        )
        
        scaler = StandardScaler(
            inputCol=features_raw_col,
            outputCol=features_scaled_col,
            withStd=True,
            withMean=True
        )
        
        prediction_col = f"prediction_{target_feature}"
        
        if model_type == "GBT":
            model = GBTRegressor(
                featuresCol=features_scaled_col,
                labelCol=target_feature,
                predictionCol=prediction_col,
                maxIter=config.GBT_PARAMS['maxIter'],
                maxDepth=config.GBT_PARAMS['maxDepth'],
                stepSize=config.GBT_PARAMS['stepSize'],
                seed=config.RANDOM_SEED
            )
        else:
            model = RandomForestRegressor(
                featuresCol=features_scaled_col,
                labelCol=target_feature,
                predictionCol=prediction_col,
                numTrees=config.RF_REGRESSION_PARAMS['numTrees'],
                maxDepth=config.RF_REGRESSION_PARAMS['maxDepth'],
                seed=config.RANDOM_SEED
            )
        
        pipeline = Pipeline(stages=[assembler, scaler, model])
        return pipeline
    
    def build_classification_model(self, target_feature: str, feature_cols: list):
        """
        Xây dựng Pipeline cho bài toán phân loại (Dự đoán Category)
        

[Image of Classification Pipeline]

        """
        print(f"\n🏷️  Building classifier for {target_feature}...")

        features_raw_col = f"features_raw_{target_feature}"
        features_scaled_col = f"features_{target_feature}"
        
        # 1. String Indexer (Biến chữ thành số)
        label_indexer = StringIndexer(
            inputCol=target_feature,
            outputCol="label", 
            handleInvalid="skip"
        )
        
        # 2. Vector Assembler
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol=features_raw_col,
            handleInvalid="skip"
        )
        
        # 3. Scaler
        scaler = StandardScaler(
            inputCol=features_raw_col,
            outputCol=features_scaled_col,
            withStd=True,
            withMean=True
        )
        
        # 4. Classifier
        classifier = RandomForestClassifier(
            featuresCol=features_scaled_col,
            labelCol="label",
            predictionCol="prediction_indexed",
            numTrees=config.RF_CLASSIFICATION_PARAMS['numTrees'],
            maxDepth=config.RF_CLASSIFICATION_PARAMS['maxDepth'],
            seed=config.RANDOM_SEED
        )
        
        # QUAN TRỌNG: Không thêm IndexToString ở đây.
        # Chúng ta sẽ thêm nó thủ công sau khi train xong để đảm bảo có labels.
        
        pipeline = Pipeline(stages=[label_indexer, assembler, scaler, classifier])
        return pipeline
    
    def build_all_models(self, feature_cols: list):
        """
        Xây dựng toàn bộ các models cần thiết
        """
        print("\n" + "="*60)
        print("🏗️  BUILDING ALL FORECAST MODELS")
        print("="*60)
        
        models = {}
        
        # Regression Models
        for target in config.CONTINUOUS_FEATURES:
            model_type = config.MODEL_SELECTION.get(target, "GBT")
            models[target] = self.build_regression_model(target, feature_cols, model_type)
        
        # Classification Models
        if hasattr(config, 'CATEGORICAL_FEATURES'):
            for target in config.CATEGORICAL_FEATURES:
                models[target] = self.build_classification_model(target, feature_cols)
        
        self.models = models
        self.feature_cols = feature_cols
        
        print(f"\n✅ Built {len(models)} models successfully!")
        print("="*60 + "\n")
        
        return models
    
    def train_all_models(self, train_df: DataFrame):
        """
        Huấn luyện toàn bộ các models
        """
        print("\n" + "="*60)
        print("🎓 TRAINING ALL MODELS")
        print("="*60)
        
        if not self.models:
            raise ValueError("Models not built yet. Call build_all_models() first.")
        
        trained_models = {}
        
        for target_feature, pipeline in self.models.items():
            print(f"\n🚂 Training model for {target_feature}...")
            try:
                # Chỉ train trên các dòng có dữ liệu label
                train_data = train_df.filter(train_df[target_feature].isNotNull())
                
                if train_data.count() == 0:
                     print(f"   ⚠️ Skipping {target_feature}: No valid training data.")
                     continue

                # Fit model (Train)
                model = pipeline.fit(train_data)
                
                # --- LOGIC XỬ LÝ RIÊNG CHO CLASSIFICATION ---
                # Nếu là biến phân loại, ta cần thêm bước IndexToString với labels cụ thể
                if hasattr(config, 'CATEGORICAL_FEATURES') and target_feature in config.CATEGORICAL_FEATURES:
                    print(f"   ℹ️  Post-processing classification model for {target_feature}...")
                    
                    # Lấy Stage StringIndexerModel (thường là cái đầu tiên - index 0)
                    # Pipeline: [StringIndexer, Assembler, Scaler, Classifier]
                    string_indexer_model = model.stages[0]
                    
                    # Lấy danh sách nhãn đã học được (VD: ['Rain', 'Clouds', 'Clear'])
                    learned_labels = string_indexer_model.labels
                    print(f"      Labels found: {learned_labels}")
                    
                    # Tạo IndexToString thủ công với labels này
                    label_converter = IndexToString(
                        inputCol="prediction_indexed",
                        outputCol=f"prediction_{target_feature}",
                        labels=learned_labels # <--- CHÌA KHÓA LÀ ĐÂY
                    )
                    
                    # Tạo PipelineModel mới bao gồm cả IndexToString
                    new_stages = model.stages + [label_converter]
                    model = PipelineModel(new_stages)
                # ----------------------------------------------

                trained_models[target_feature] = model
                print(f"   ✅ Training complete for {target_feature}")
                
            except Exception as e:
                print(f"   ❌ Error training {target_feature}: {e}")
                # Import traceback để debug nếu cần
                # import traceback
                # traceback.print_exc()
        
        return trained_models
    
    def save_all_models(self, trained_models: dict, base_path: str):
        """
        Lưu các model đã train xuống ổ cứng
        """
        print(f"\n💾 Saving models to {base_path}...")
        
        for target_feature, model in trained_models.items():
            try:
                model_path = os.path.join(base_path, f"{target_feature}_model")
                model.write().overwrite().save(model_path)
                print(f"   Saved: {model_path}")
            except Exception as e:
                print(f"   ⚠️  Could not save model for {target_feature}: {e}")

if __name__ == "__main__":
    print("Models module loaded.")