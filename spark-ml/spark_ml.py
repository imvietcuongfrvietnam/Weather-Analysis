from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# --- CẤU HÌNH ---
# ⚠️ Đảm bảo HDFS Cluster của bạn đã chạy và Spark đã được cấu hình để kết nối HDFS (HDFS_CONF)
# Đường dẫn dữ liệu mẫu (Giả sử bạn đã lưu dữ liệu thô vào HDFS)
#HDFS_INPUT_PATH = "hdfs://namenode:9000/nyc_data/weather/raw_all_years.csv"
# Đường dẫn lưu Model đã huấn luyện
#HDFS_MODEL_OUTPUT = "hdfs://namenode:9000/models/weather_rf_classifier"

# 1. KHỞI TẠO SPARK SESSION
spark = SparkSession.builder \
    .appName("WeatherClassifierTraining") \
    .master("local[*]") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session đã khởi động.")


# --- 2. LOGIC NGHIỆP VỤ (Hàm Gom nhóm của bạn) ---
def define_weather_group(desc):
    """Gom nhóm thời tiết (Chuyển từ Pandas logic sang PySpark UDF)"""
    desc = str(desc).lower()
    
    # Nhóm 1: Mưa/Bão/Tuyết (Precipitation)
    precipitation_keywords = ['rain', 'snow', 'thunderstorm', 'drizzle', 'sleet', 'squall', 'shower']
    if any(x in desc for x in precipitation_keywords):
        return 'Precipitation'
    
    # Nhóm 2: Mây/Sương/Bụi (Cloudy/Fog)
    cloudy_keywords = ['cloud', 'fog', 'mist', 'haze', 'smoke', 'dust', 'sand', 'ash']
    if any(x in desc for x in cloudy_keywords):
        return 'Cloudy/Fog'
    
    # Nhóm 3: Trời quang (Clear)
    elif 'clear' in desc:
        return 'Clear'
    
    else:
        return 'Cloudy/Fog'

# Đăng ký hàm gom nhóm thành UDF (User Defined Function)
define_weather_group_udf = udf(define_weather_group, StringType())


# --- 3. ĐỌC VÀ CHUẨN BỊ DỮ LIỆU (Giả định đọc từ CSV đã đẩy lên HDFS) ---
try:
    df_raw = spark.read.csv(HDFS_INPUT_PATH, header=True, inferSchema=True)
except Exception as e:
    print(f"❌ Lỗi đọc HDFS: {e}. Vui lòng kiểm tra lại {HDFS_INPUT_PATH}")
    # Nếu lỗi, tạo DF giả để code chạy tiếp
    data = [
        (15.0, 60.0, 1010.0, 180.0, 5.0, 'light rain'),
        (25.5, 75.0, 1015.0, 90.0, 1.2, 'sky is clear'),
        (10.1, 95.0, 1005.0, 270.0, 0.5, 'fog')
    ]
    columns = ['temperature', 'humidity', 'pressure', 'wind_direction', 'wind_speed', 'weather_desc']
    df_raw = spark.createDataFrame(data, columns)


# 4. CHUYỂN ĐỔI (TRANSFORMATION)
df = df_raw.withColumn('weather_group', define_weather_group_udf(col('weather_desc')))

# Loại bỏ các hàng có giá trị null
df = df.dropna(subset=['temperature', 'humidity', 'pressure', 'wind_direction', 'wind_speed', 'weather_group'])

# --- BƯỚC 5: XÂY DỰNG PIPELINE ML ---

# A. Chuyển đổi nhãn chuỗi thành số (StringIndexer)
# Đây là bước bắt buộc cho các thuật toán ML
indexer = StringIndexer(inputCol="weather_group", outputCol="label")

# B. Gom các Features lại thành 1 Vector (VectorAssembler)
features = ['temperature', 'humidity', 'pressure', 'wind_direction', 'wind_speed']
assembler = VectorAssembler(inputCols=features, outputCol="features_unscaled")

# C. Chuẩn hóa dữ liệu (StandardScaler)
# Quan trọng cho các mô hình dựa trên khoảng cách
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features",
                        withStd=True, withMean=False)

# D. Mô hình Phân loại (RandomForestClassifier)
# Thay max_depth=20 và n_estimators=100 như trong code sklearn của bạn
rf = RandomForestClassifier(labelCol="label", featuresCol="features",
                            numTrees=100, maxDepth=20, 
                            seed=42)

# E. Định nghĩa các bước trong Pipeline
pipeline = Pipeline(stages=[assembler, scaler, indexer, rf])


# --- 6. HUẤN LUYỆN VÀ ĐÁNH GIÁ ---

# Chia Train/Test (Spark không dùng stratify đơn giản như sklearn, dùng randomSplit)
(trainingData, testData) = df.randomSplit([0.85, 0.15], seed=42)
print("🚀 Bắt đầu huấn luyện mô hình Random Forest trên Spark Cluster...")

# Huấn luyện mô hình
model = pipeline.fit(trainingData)
print("✅ Huấn luyện hoàn tất!")

# Dự đoán trên tập Test
predictions = model.transform(testData)

# Đánh giá (Sử dụng F1-Score là chuẩn)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
f1_score = evaluator.evaluate(predictions)
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})

print("\n--- KẾT QUẢ ĐÁNH GIÁ TRÊN SPARK ---")
print(f"Độ chính xác (Accuracy): {accuracy:.4f}")
print(f"F1 Score (Độ cân bằng): {f1_score:.4f}")


# --- 7. LƯU CHECKPOINT MÔ HÌNH VÀO HDFS ---
try:
    print(f"\n💾 Đang lưu mô hình vào HDFS tại: {HDFS_MODEL_OUTPUT}")
    # Xóa thư mục cũ nếu tồn tại
    spark._jsc.hadoopConfiguration().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    if fs.exists(spark._jvm.org.apache.hadoop.fs.Path(HDFS_MODEL_OUTPUT)):
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(HDFS_MODEL_OUTPUT), True)
        print("   -> Đã xóa phiên bản cũ.")
        
    # Lưu mô hình (Tự động lưu tất cả các bước của Pipeline)
    model.write().overwrite().save(HDFS_MODEL_OUTPUT)
    print("✅ Lưu mô hình thành công! Mô hình đã được checkpoint.")
    
except Exception as e:
    print(f"❌ Lỗi lưu mô hình vào HDFS: {e}")


spark.stop()