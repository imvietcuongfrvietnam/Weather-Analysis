# 🚀 Cách Chạy Project

## 📋 Bước 1: Generate Test Data (CHỈ LẦN ĐẦU)

**⚠️ Quan trọng:** Chỉ cần chạy **1 LẦN** để tạo data, hoặc khi muốn data mới!

### Windows:

```bash
generate_data.bat
```

### Git Bash:

```bash
./generate_data.sh
```

Hoặc chạy trực tiếp:

```bash
py -3.11 generate_data.py
```

📁 **Kết quả:** Tạo folder `./data/` chứa:

- `weather_data.json` (1000 records)
- `311_requests.json` (500 records)
- `taxi_trips.json` (800 records)
- `collisions.json` (300 records)
- `metadata.json`

---

## ⚡ Bước 2: Chạy ETL Pipeline

### Windows:

```bash
run.bat
```

### Git Bash (Windows):

```bash
./run.sh
```

hoặc

```bash
bash run.sh
```

💡 **Lợi ích:**

- Không cần generate data mỗi lần chạy
- Nhanh hơn 10x
- Data nhất quán cho testing
- Dễ debug và reproduce lỗi

---

## 📝 Cách Chạy Thủ Công

### Windows CMD/PowerShell:

```bash
set PYTHONIOENCODING=utf-8
py -3.11 main_etl.py
```

### Git Bash:

```bash
export PYTHONIOENCODING=utf-8
py -3.11 main_etl.py
```

---

## ❓ Tại Sao Phải `py -3.11`?

Hệ thống có **2 phiên bản Python**:

- ✅ **Python 3.11** - Tương thích với PySpark 4.0.1
- ❌ **Python 3.13** (mặc định) - Không tương thích với PySpark

Kiểm tra các phiên bản Python:

```bash
py --list
```

Kết quả:

```
 -V:3.13 *        Python 3.13 (64-bit)  ← Mặc định (dấu *)
 -V:3.11          Python 3.11 (64-bit)  ← Phiên bản cần dùng
```

---

## 🔧 Cách Đặt Python 3.11 Làm Mặc Định (Tùy chọn)

### Cách 1: Chỉnh sửa thủ công

1. Mở Windows Settings
2. Vào: **Apps > Apps & features > App execution aliases**
3. **TẮT** các alias: `python.exe` và `python3.exe`
4. Thêm Python 3.11 vào PATH:
   - Mở: **System Properties > Environment Variables**
   - Thêm vào PATH:
     ```
     C:\Users\[YOUR_USERNAME]\AppData\Local\Programs\Python\Python311
     C:\Users\[YOUR_USERNAME]\AppData\Local\Programs\Python\Python311\Scripts
     ```
   - Đảm bảo Python 3.11 ở **TRÊN CÙNG** trong danh sách PATH

### Cách 2: Chạy script tự động

```bash
setup_python_default.bat
```

---

## 📊 Xem Kết Quả

### Input Data (Generated Once):

```
./data/
├── weather_data.json              ← Raw weather data (1000 records)
├── 311_requests.json              ← Raw 311 requests (500 records)
├── taxi_trips.json                ← Raw taxi trips (800 records)
├── collisions.json                ← Raw collision data (300 records)
└── metadata.json                  ← Data information
```

### Output Data (After ETL):

**📁 Cleaned Data (JSON format - for inspection):**

```
./output/
├── weather_cleaned.json           ← Cleaned weather data (JSON readable)
├── 311_requests_cleaned.json      ← Cleaned 311 requests
├── taxi_trips_cleaned.json        ← Cleaned taxi trips
├── collisions_cleaned.json        ← Cleaned collision data
└── integrated_final.json          ← Final enriched & integrated data
```

**📁 Legacy Output (CSV/Parquet - for compatibility):**

```
./fake_output/
├── stage_1_cleaned_weather/       ← Data sau bước CLEAN
├── stage_2_normalized_weather/    ← Data sau bước NORMALIZE
├── stage_3_enriched_weather/      ← Data sau bước ENRICH
└── weather_disaster_integrated/   ← Data cuối cùng (38 features)
```

**💡 Xem cleaned data:**

```bash
# Windows Explorer
explorer output

# Git Bash
start output

# Hoặc dùng text editor
code output/weather_cleaned.json
```

**🔍 So sánh INPUT vs OUTPUT:**

- **INPUT** (`./data/*.csv`): Raw data từ generate_data.py
- **OUTPUT** (`./output/*.json`): Cleaned data với type casting, validation, null handling

---

## 🎯 Kiểm Tra Nhanh

Chỉ muốn test xem Python và PySpark hoạt động không?

```bash
py -3.11 test_simple.py
```

---

## 📦 Dependencies

```bash
# Cài đặt dependencies (nếu chưa có)
py -3.11 -m pip install -r requirements.txt
```

**requirements.txt** bao gồm:

- pyspark==4.0.1
- pandas>=2.0.0
- numpy>=1.24.0
- pyarrow>=10.0.0

---

## 🔄 Chuyển Sang Kafka/HDFS/Elasticsearch (Tương Lai)

### 📥 INPUT: Chuyển từ CSV sang Kafka

**Hiện tại:** CSV files (batch) → Dễ test, không cần setup
**Tương lai:** Kafka streams (real-time) → Production-ready

**File:** `main_etl.py`

```python
# Thay đổi từ:
reader = DataReader(spark, source_type="json")

# Sang:
reader = DataReader(spark, source_type="kafka")
```

**Kafka Configuration (Khi ready):**

```python
# In readers/real_data_reader.py
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-topic") \
    .option("startingOffsets", "earliest") \
    .load()
```

**📝 Topics cần thiết:**

- `weather-topic` - Weather data stream
- `311-topic` - 311 requests stream
- `taxi-topic` - Taxi trips stream
- `collision-topic` - Collision data stream

---

### 💾 OUTPUT: Chuyển từ JSON sang HDFS/Elasticsearch

**Hiện tại:** JSON files → Readable, dễ inspect
**Tương lai:** HDFS + Elasticsearch → Scalable storage + search

**File:** `main_etl.py`

```python
# Thay đổi từ:
data_writer = DataWriter(output_type="json")

# Sang HDFS:
data_writer = DataWriter(output_type="hdfs")

# Hoặc Elasticsearch:
data_writer = DataWriter(output_type="elasticsearch")
```

**HDFS Configuration (Khi ready):**

```python
# In writers/real_data_writer.py
df.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("date") \
    .save("hdfs://namenode:9000/data/weather_cleaned")
```

**Elasticsearch Configuration (Khi ready):**

```python
# In writers/real_data_writer.py
df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "weather-disaster-nyc") \
    .mode("append") \
    .save()
```

**💡 Architecture Flow:**

```
┌────────────────────────────────────────────────────────────┐
│  CURRENT (Development)                                      │
├────────────────────────────────────────────────────────────┤
│  CSV Files → Spark ETL → JSON Files                        │
│  (./data/)              (./output/)                         │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│  FUTURE (Production)                                        │
├────────────────────────────────────────────────────────────┤
│  Kafka Streams → Spark ETL → HDFS (storage)               │
│                              → Elasticsearch (search/viz)   │
└────────────────────────────────────────────────────────────┘
```
