# Spark ETL - Hướng dẫn sử dụng

## 🎯 Giới thiệu

Đây là phần **Spark ETL Batch** cho project "Phân tích dữ liệu thời tiết dự báo thiên tai - NYC"

### Chức năng:

- ✅ Đọc data từ 4 nguồn: Weather, 311 Requests, Taxi Trips, Collisions
- ✅ Clean data (remove nulls, duplicates, outliers)
- ✅ Normalize (chuẩn hóa units, formats)
- ✅ Enrich (tính disaster risk, traffic impact, ML features)
- ✅ Write to HDFS + Elasticsearch

### Hiện tại:

- ✅ Code hoàn chỉnh với **FAKE I/O**
- ✅ Có thể chạy standalone để test
- ⏳ Chưa tích hợp Kafka/HDFS/ES thật (sẽ làm sau)

---

## 📂 Cấu trúc Project

```
spark_etl_weather_disaster/
├── README.md                  # File này
├── GUIDE.md                   # Hướng dẫn chi tiết
├── main_etl.py                # Main pipeline (CHẠY FILE NÀY)
│
├── schemas/                   # Data schemas
│   ├── __init__.py
│   └── data_schemas.py        # 4 source schemas + processed schema
│
├── readers/                   # Data readers
│   ├── __init__.py
│   └── data_readers.py        # FakeDataReader + RealDataReader template
│
├── transformations/           # ETL transformations
│   ├── __init__.py
│   ├── cleaning.py            # Clean functions
│   ├── normalization.py       # Normalize functions
│   └── enrichment.py          # Enrich functions
│
├── writers/                   # Data writers
│   ├── __init__.py
│   └── data_writers.py        # FakeDataWriter + RealDataWriter template
│
└── fake_output/               # Output folder (tạo tự động)
```

---

## 🚀 Cách chạy

### Yêu cầu:

```bash
# Install PySpark
pip install pyspark

# Optional
pip install pandas numpy
```

### Chạy ETL Pipeline:

```bash
cd spark_etl_weather_disaster
python main_etl.py
```

### Output:

```
🚀 SPARK ETL - WEATHER & DISASTER PREDICTION - NYC
================================================================================

📖 STEP 1: READING DATA FROM 4 SOURCES
📊 [FAKE] Generating 1000 weather records...
   ✅ Generated 1000 weather records
📊 [FAKE] Generating 500 311 service requests...
   ✅ Generated 500 311 requests
...

🧹 STEP 2: CLEANING DATA
...

📏 STEP 3: NORMALIZING DATA
...

✨ STEP 4: ENRICHING DATA
...

💾 STEP 5: WRITING DATA
...

✅ ETL PIPELINE COMPLETED SUCCESSFULLY!
```

---

## 📊 Dữ liệu được xử lý

### 1. Weather Data (Kaggle)

- Temperature, humidity, pressure, wind
- Precipitation (rain + snow)
- Weather conditions

### 2. 311 Service Requests (NYC)

- Complaints/requests (tree damage, flooding, etc.)
- Location, borough
- Response time

### 3. Taxi Trips (NYC TLC)

- Pickup/dropoff locations & times
- Trip distance, duration, speed
- Fare information

### 4. Motor Vehicle Collisions (NYC)

- Crash location, time
- Injuries, fatalities
- Contributing factors (weather-related)

---

## 🔄 Quy trình ETL

```
READ (Fake)
    ↓
CLEAN (Remove nulls, duplicates, outliers)
    ↓
NORMALIZE (Standardize units, formats)
    ↓
ENRICH (Calculate risk scores, ML features)
    ↓
INTEGRATE (Join 4 sources by time/location)
    ↓
WRITE (Fake HDFS + Fake ES)
```

---

## 📈 Features được tính toán

### Disaster Risk Score (0-100)

- Precipitation risk
- Wind speed risk
- Temperature extremes
- Low pressure (storms)
- Weather severity

### Traffic Impact Score (0-100)

- Trip count reduction
- Collision increase
- Casualties

### ML Features

- Time-based: hour, day_of_week, season, is_weekend, is_rush_hour
- Weather comfort index
- 24h rolling averages
- Weather-traffic correlations

---

## 🔧 Tích hợp với Pipeline thật

### Bước 1: Thay Fake Readers

```python
# Trong main_etl.py, thay:
from readers.data_readers import FakeDataReader
reader = FakeDataReader(spark)

# Thành:
from readers.data_readers import RealDataReader
reader = RealDataReader(spark)

# Và trong RealDataReader, implement:
def read_from_kafka(self, topic):
    return self.spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "your-kafka:9092") \
        .option("subscribe", topic) \
        .load()
```

### Bước 2: Thay Fake Writers

```python
# Thay:
from writers.data_writers import FakeDataWriter
writer = FakeDataWriter()

# Thành:
from writers.data_writers import RealDataWriter
writer = RealDataWriter()

# Và implement write_to_hdfs(), write_to_elasticsearch()
```

### Bước 3: Config

- Kafka bootstrap servers
- HDFS namenode address
- Elasticsearch nodes/port

---

## 📝 TODO List

- [ ] Integrate real Kafka readers
- [ ] Integrate real HDFS writers
- [ ] Integrate real Elasticsearch writers
- [ ] Add error handling & retry logic
- [ ] Add comprehensive logging
- [ ] Add unit tests
- [ ] Add data quality checks
- [ ] Optimize performance (partitioning, caching)
- [ ] Deploy to Spark cluster
- [ ] Schedule with Airflow

---

## 💡 Tips

1. **Testing**: Chạy với fake data trước để test logic
2. **Debugging**: Check Spark UI (localhost:4040 khi chạy)
3. **Performance**: Sử dụng `.cache()` cho data được reuse nhiều lần
4. **Partitioning**: Partition by date/hour khi write HDFS

---

## 🎓 Kết quả học được

Sau khi hoàn thành phần này, bạn sẽ hiểu:

- ✅ Spark ETL pipeline design
- ✅ Data cleaning, normalization, enrichment
- ✅ Multi-source data integration
- ✅ Feature engineering cho ML
- ✅ Modular code organization
- ✅ Fake I/O cho testing trước khi deploy

---

## 📞 Support

Nếu có vấn đề, check:

1. Spark logs
2. Python traceback
3. Data schema mismatches
4. Null pointer errors

---

**Good luck với project! 🚀**
