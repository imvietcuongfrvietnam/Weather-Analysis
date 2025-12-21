# HƯỚNG DẪN DEPLOYMENT - KHI CÓ KAFKA VÀ MINIO SERVER

## 📋 Thông Tin Cần Xin Từ Team

### 🔴 **Từ Người Setup Kafka:**
```
1. Kafka Bootstrap Servers: ________________________
   Ví dụ: "kafka1.company.com:9092,kafka2.company.com:9092"

2. Topic Names:
   - Weather topic:    ________________________
   - 311 topic:        ________________________
   - Taxi topic:       ________________________
   - Collision topic:  ________________________

3. Consumer Group ID: ________________________ (optional)
```

### 🔵 **Từ Người Setup MinIO:**
```
1. MinIO Endpoint: ________________________
   Ví dụ: "minio.company.com:9000"

2. Access Key: ________________________

3. Secret Key: ________________________

4. Bucket Name: ________________________ (hoặc dùng mặc định "weather-data")

5. Dùng SSL/HTTPS? [ ] Yes  [ ] No
```

---

## 🚀 DEPLOYMENT - 3 BƯỚC ĐƠN GIẢN

### **Bước 1: Cấu hình Kafka**

Mở file `kafka_config.py`, sửa các dòng sau:

```python
# Dòng 17-18: Thay Kafka servers
KAFKA_BOOTSTRAP_SERVERS = "your-kafka-server:9092"  # ← Thay đây

# Dòng 24-29: Thay topic names (nếu khác)
KAFKA_TOPICS = {
    "weather": "nyc-weather-raw",       # ← Thay nếu khác
    "311": "nyc-311-data",              # ← Thay nếu khác
    "taxi": "nyc-taxi-data",            # ← Thay nếu khác
    "collision": "nyc-collision-data"   # ← Thay nếu khác
}

# Dòng 35: Consumer group ID (optional, có thể giữ nguyên)
KAFKA_GROUP_ID = "spark-weather-etl-consumer"
```

### **Bước 2: Cấu hình MinIO**

Mở file `minio_config.py`, sửa các dòng sau:

```python
# Dòng 21: Thay endpoint
MINIO_ENDPOINT = "your-minio-server:9000"  # ← Thay đây

# Dòng 26-27: Thay credentials
MINIO_ACCESS_KEY = "your-access-key"       # ← Thay đây
MINIO_SECRET_KEY = "your-secret-key"       # ← Thay đây

# Dòng 30: Bật SSL nếu dùng HTTPS
MINIO_SECURE = False  # ← Đổi True nếu dùng HTTPS

# Dòng 37: Bucket name (optional)
MINIO_BUCKET = "weather-data"  # ← Thay nếu khác
```

### **Bước 3: Bật Kafka và MinIO trong Main ETL**

Mở file `main_etl.py`, sửa 2 dòng:

```python
# Dòng 66: Bật Kafka reader (chọn mode: "batch" hoặc "streaming")
reader = DataReader(spark, source_type="kafka", kafka_mode="batch")
# Khuyến nghị: Dùng "batch" mode cho dễ, dùng "streaming" cho real-time

# Dòng 137: Bật MinIO writer  
data_writer = DataWriter(output_type="minio")
```

**💡 Chọn Kafka Mode:**
- `kafka_mode="batch"` - Đơn giản, tương thích code hiện tại (khuyến nghị)
- `kafka_mode="streaming"` - Real-time, cần sửa thêm code (nâng cao)

Xem chi tiết: `KAFKA_MODES.md`

---

## ✅ TEST KẾT NỐI

### Test Kafka Config:
```bash
python3 kafka_config.py
```
**Kết quả mong đợi:**
```
================================================================================
📡 KAFKA CONFIGURATION
================================================================================
Bootstrap Servers: your-kafka-server:9092
Topics:
  - weather    : nyc-weather-raw
  - 311        : nyc-311-data
  ...
================================================================================
```

### Test MinIO Config:
```bash
python3 minio_config.py
```
**Kết quả mong đợi:**
```
================================================================================
📦 MINIO CONFIGURATION
================================================================================
Endpoint:     your-minio-server:9000
Bucket:       weather-data
...
================================================================================
```

### Chạy ETL Pipeline:
```bash
python3 main_etl.py
```

---

## 🐛 TROUBLESHOOTING

### Lỗi Kafka Connection:
```
❌ Error connecting to Kafka: ...
```
**Giải pháp:**
- Check Kafka server đang chạy
- Verify bootstrap servers URL đúng
- Check network/firewall có block không
- Test connection: `telnet kafka-server 9092`

### Lỗi MinIO Connection:
```
⚠️ Could not connect to MinIO: ...
```
**Giải pháp:**
- Check MinIO server đang chạy
- Verify endpoint, access key, secret key đúng
- Test connection: Mở browser `http://minio-endpoint:9000`
- Check SSL setting (`MINIO_SECURE`) đúng chưa

### ETL chạy nhưng không thấy data:
- Check Kafka có data đang stream không
- Check MinIO bucket đã tạo chưa
- Xem logs chi tiết trong console

---

## 📊 KẾT QUẢ MONG ĐỢI

Sau khi chạy thành công:

### Kafka → Spark:
```
📡 Connecting to Kafka...
📥 Subscribing to topic: nyc-weather-raw
✅ Connected to Kafka successfully!
📊 Reading weather data from kafka...
```

### Spark → MinIO:
```
📦 Adding MinIO/S3 configuration to Spark...
✅ Bucket 'weather-data' exists
💾 Writing weather data to minio...
✅ Uploaded 1000 records to MinIO
📦 Bucket: weather-data
📁 Path: cleaned/weather/data.parquet
```

---

## 📝 TÓM TẮT

| Bước | File cần sửa | Số dòng cần sửa | Khó hay Dễ? |
|------|--------------|-----------------|-------------|
| 1 | `kafka_config.py` | ~5 dòng | ✅ Dễ |
| 2 | `minio_config.py` | ~4 dòng | ✅ Dễ |
| 3 | `main_etl.py` | 2 dòng | ✅ Rất dễ |

**Tổng cộng: Chỉ ~11 dòng code cần thay đổi!** 🎉
