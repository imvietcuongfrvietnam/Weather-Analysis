# ⚠️ SCHEMA vs TRANSFORMATIONS - COMPATIBILITY CHECK

## 🔍 KIỂM TRA CHI TIẾT

Đã check schemas và transformations - phát hiện **1 VẤN ĐỀ QUAN TRỌNG**:

---

## ❌ **VẤN ĐỀ: WEATHER DATA FIELDS**

### **Schema hiện tại** (data_schemas.py):

```python
weather_schema_long = StructType([
    StructField("datetime", TimestampType(), False),
    StructField("city", StringType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("weather_description", StringType(), True),
    # ❌ THIẾU: rain_1h, snow_1h, clouds_all
])
```

### **Transformations đang dùng** (cleaning.py):

```python
# Line 32 - cleaning.py:
df = df.fillna({"rain_1h": 0.0, "snow_1h": 0.0})  # ❌ Fields không tồn tại!
```

### **Transformations đang dùng** (normalization.py):

```python
# Line 28 - normalization.py:
df = df.withColumn("precipitation_mm", col("rain_1h") + col("snow_1h"))  # ❌ Error!
```

---

## ✅ **CÁC NGUỒN KHÁC - OK**

### **1. 311 Service Requests** ✅

| Transformation Uses                | Schema Has | Status |
| ---------------------------------- | ---------- | ------ |
| `unique_key`                       | ✅         | OK     |
| `created_date`                     | ✅         | OK     |
| `closed_date`                      | ✅         | OK     |
| `complaint_type`                   | ✅         | OK     |
| `borough`, `latitude`, `longitude` | ✅         | OK     |

### **2. Taxi Trips** ✅

| Transformation Uses         | Schema Has (2016) | Status           |
| --------------------------- | ----------------- | ---------------- |
| `tpep_pickup_datetime`      | ✅                | OK               |
| `tpep_dropoff_datetime`     | ✅                | OK               |
| `trip_distance`             | ✅                | OK               |
| `fare_amount`               | ✅                | OK               |
| `passenger_count`           | ✅                | OK               |
| `pickup_latitude/longitude` | ✅                | OK (2016 schema) |

### **3. Collisions** ✅

| Transformation Uses             | Schema Has | Status |
| ------------------------------- | ---------- | ------ |
| `crash_date`                    | ✅         | OK     |
| `crash_time`                    | ✅         | OK     |
| `borough`                       | ✅         | OK     |
| `latitude`, `longitude`         | ✅         | OK     |
| `number_of_persons_injured`     | ✅         | OK     |
| `number_of_persons_killed`      | ✅         | OK     |
| `contributing_factor_vehicle_1` | ✅         | OK     |

---

## 🔧 **FIX REQUIRED**

### **Option 1: Update Schema (RECOMMENDED)**

Add missing fields to weather schema:

```python
weather_schema_long = StructType([
    StructField("datetime", TimestampType(), False),
    StructField("city", StringType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("weather_description", StringType(), True),
    # ✅ ADD THESE:
    StructField("rain_1h", DoubleType(), True),       # mm
    StructField("snow_1h", DoubleType(), True),       # mm
    StructField("clouds_all", IntegerType(), True),   # %
])
```

### **Option 2: Update Transformations**

Remove references to non-existent fields:

```python
# Instead of:
df = df.fillna({"rain_1h": 0.0, "snow_1h": 0.0})  # ❌

# Use:
# Skip this step if fields don't exist
```

---

## 📊 **SUMMARY**

| Data Source      | Schema Match       | Transformation Match        | Status        |
| ---------------- | ------------------ | --------------------------- | ------------- |
| **Weather**      | ⚠️ Missing fields  | ❌ Uses non-existent fields | **NEEDS FIX** |
| **311 Requests** | ✅ Complete        | ✅ All fields exist         | **OK**        |
| **Taxi Trips**   | ✅ Complete (2016) | ✅ All fields exist         | **OK**        |
| **Collisions**   | ✅ Complete        | ✅ All fields exist         | **OK**        |

---

## ✅ **RECOMMENDATION**

**Fix Option 1 (Best):** Add missing fields to weather schema

- Pros: Kaggle weather data HAS these fields
- Cons: None
- Action: Update `schemas/data_schemas.py`

**Action Required:**

1. Add `rain_1h`, `snow_1h`, `clouds_all` to weather schema
2. Update fake reader to generate these fields
3. Test transformations

---

## 🎯 **VERDICT**

**3 out of 4 sources: ✅ MATCH**

- 311: ✅
- Taxi: ✅
- Collision: ✅

**1 out of 4 sources: ⚠️ NEEDS FIX**

- Weather: ⚠️ (missing 3 fields)

**Fix này đơn giản - chỉ cần add 3 fields vào weather schema!**
