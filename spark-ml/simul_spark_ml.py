from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, StructType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import random
import numpy as np

# --- CẤU HÌNH & CHUẨN BỊ DỮ LIỆU GIẢ LẬP ---
DATASET_SIZE = 1000  # Kích thước dữ liệu giả lập để Training
WEATHER_OPTS = ['light rain', 'sky is clear', 'fog', 'thunderstorm', 'broken clouds', 'mist']

# 1. KHỞI TẠO SPARK SESSION
spark = SparkSession.builder \
    .appName("WeatherClassifierMockTraining") \
    .master("local[*]") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session đã khởi động.")

# Hàm tạo dữ liệu giả lập lớn (để thay thế HDFS)
def generate_mock_data(size):
    data = []
    for _ in range(size):
        # Tạo dữ liệu ngẫu nhiên với phân phối hơi nghiêng về các trường hợp thực tế
        desc = random.choice(WEATHER_OPTS)
        
        if 'rain' in desc or 'thunderstorm' in desc: # Trường hợp Mưa/Bão
            temp = random.uniform(5, 20)
            humidity = random.uniform(80, 100)
            wind = random.uniform(5, 15)
        elif 'clear' in desc: # Trường hợp Trời quang
            temp = random.uniform(15, 30)
            humidity = random.uniform(30, 60)
            wind = random.uniform(0, 5)
        else: # Trường hợp Mây/Sương
            temp = random.uniform(10, 25)
            humidity = random.uniform(60, 90)
            wind = random.uniform(2, 10)
            
        data.append({
            'temperature': round(temp, 2),
            'humidity': round(humidity, 2),
            'pressure': round(random.uniform(1000, 1025), 2),
            'wind_direction': random.randint(0, 360),
            'wind_speed': round(wind, 2),
            'weather_desc': desc
        })
    return spark.createDataFrame(data)

# Đọc dữ liệu (BƯỚC NÀY THAY THẾ CHO spark.read.csv(HDFS_INPUT_PATH))
df_raw = generate_mock_data(DATASET_SIZE)

# --- 2. LOGIC NGHIỆP VỤ (Hàm Gom nhóm của bạn) ---
def define_weather_group(desc):
    desc = str(desc).lower()
    
    precipitation_keywords = ['rain', 'snow', 'thunderstorm', 'drizzle', 'sleet', 'squall', 'shower']
    if any(x in desc for x in precipitation_keywords):
        return 'Precipitation'
    
    cloudy_keywords = ['cloud', 'fog', 'mist', 'haze', 'smoke', 'dust', 'sand', 'ash']
    if any(x in desc for x in cloudy_keywords):
        return 'Cloudy/Fog'
    
    elif 'clear' in desc:
        return 'Clear'
    
    else:
        return 'Cloudy/Fog'

# Đăng ký hàm gom nhóm thành UDF (User Defined Function)
define_weather_group_udf = udf(define_weather_group, StringType())


# --- 3. CHUYỂN ĐỔI (TRANSFORMATION) ---
df = df_raw.withColumn('weather_group', define_weather_group_udf(col('weather_desc')))

# Loại bỏ các hàng có giá trị null (để code không lỗi)
df = df.dropna()


# --- BƯỚC 4: XÂY DỰNG PIPELINE ML ---

# A. Chuyển đổi nhãn chuỗi thành số (StringIndexer)
indexer = StringIndexer(inputCol="weather_group", outputCol="label")

# B. Gom các Features lại thành 1 Vector (VectorAssembler)
features = ['temperature', 'humidity', 'pressure', 'wind_direction', 'wind_speed']
assembler = VectorAssembler(inputCols=features, outputCol="features_unscaled")

# C. Chuẩn hóa dữ liệu (StandardScaler)
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features",
                        withStd=True, withMean=False)

# D. Mô hình Phân loại (RandomForestClassifier)
rf = RandomForestClassifier(labelCol="label", featuresCol="features",
                            numTrees=100, maxDepth=20, 
                            seed=42)

# E. Định nghĩa các bước trong Pipeline
pipeline = Pipeline(stages=[assembler, scaler, indexer, rf])


# --- 5. HUẤN LUYỆN VÀ ĐÁNH GIÁ ---

# Chia Train/Test 
(trainingData, testData) = df.randomSplit([0.85, 0.15], seed=42)
print("🚀 Bắt đầu huấn luyện mô hình Random Forest trên Spark (Local Mode)...")

# Huấn luyện mô hình
model = pipeline.fit(trainingData)
print("✅ Huấn luyện hoàn tất!")

# Dự đoán trên tập Test
predictions = model.transform(testData)

# Đánh giá
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
f1_score = evaluator.evaluate(predictions)
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})

print("\n--- KẾT QUẢ ĐÁNH GIÁ TRÊN SPARK ---")
print(f"Độ chính xác (Accuracy): {accuracy:.4f}")
print(f"F1 Score (Độ cân bằng): {f1_score:.4f}")


# --- 6. (BỎ QUA HDFS) Chỉ cần dừng Session ---
# BƯỚC NÀY KHÔNG CÒN LƯU VÀO HDFS NỮA VÌ TA ĐANG DEBUG LỖI HDFS
# Sau khi sửa xong lỗi HDFS, bạn sẽ bật lại đoạn code lưu model.
print("\n💡 Đã hoàn tất kiểm tra logic ML. Dừng Spark Session.")
spark.stop()