# KAFKA MODES - HƯỚNG DẪN CHỌN CHẾ ĐỘ

## 🔄 2 MODES AVAILABLE

### 📦 **BATCH MODE** (Khuyến nghị cho bắt đầu)

**Đặc điểm:**
- ✅ Đọc Kafka theo micro-batches (chunks)
- ✅ Tương thích với code ETL hiện tại
- ✅ Có thể dùng `.count()`, `.show()`, `.write()`
- ✅ Đơn giản, dễ debug
- ⚠️ Không real-time 100% (có độ trễ vài giây)

**Khi nào dùng:**
- Xử lý dữ liệu định kỳ (mỗi 5-10 phút)
- Không cần real-time tức thì
- Team chưa quen Spark Streaming
- Development/testing

**Cách bật:**
```python
# File: main_etl.py, line 66
reader = DataReader(spark, source_type="kafka", kafka_mode="batch")
```

---

### ⚡ **STREAMING MODE** (Advanced - Real-time)

**Đặc điểm:**
- ⚡ Real-time processing (xử lý ngay khi có data)
- 🚀 Latency thấp (vài milliseconds)
- ⚠️ KHÔNG thể dùng `.count()`, `.show()`, `.write()`
- ⚠️ Phải dùng `.writeStream`, checkpoint, trigger
- ⚠️ Code phức tạp hơn

**Khi nào dùng:**
- Cần real-time alerts (thời tiết nguy hiểm, disaster)
- Low latency requirements
- Production với Lambda Architecture
- Team đã quen Spark Streaming

**Cách bật:**
```python
# File: main_etl.py, line 66
reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
```

**⚠️ LƯU Ý:** Nếu dùng streaming mode, phải sửa thêm:
- Thay tất cả `.write()` → `.writeStream()`
- Thêm checkpoint location
- Thêm trigger policy
- Bỏ `.count()`, `.show()` hoặc dùng `.writeStream.format("console")`

---

## 📊 SO SÁNH

| Tiêu chí | Batch Mode | Streaming Mode |
|----------|------------|----------------|
| **Độ phức tạp** | ⭐ Đơn giản | ⭐⭐⭐ Phức tạp |
| **Real-time** | ⚠️ Độ trễ vài giây | ✅ Real-time |
| **Tương thích code hiện tại** | ✅ 100% | ❌ Cần sửa nhiều |
| **Debug** | ✅ Dễ | ⚠️ Khó hơn |
| **Throughput** | ⭐⭐⭐ Cao | ⭐⭐ Trung bình |
| **Latency** | ⭐⭐ Vài giây | ⭐⭐⭐ Milliseconds |

---

## 🎯 KHUYẾN NGHỊ

### Bắt đầu với BATCH MODE:
```python
reader = DataReader(spark, source_type="kafka", kafka_mode="batch")
```

### Sau đó nâng cấp lên STREAMING MODE khi cần:
```python
reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
# + Phải sửa thêm ETL pipeline để support streaming
```

---

## 💡 CÁCH SWITCH GIỮA 2 MODES

Chỉ cần sửa **1 dòng** trong `main_etl.py`:

```python
# BATCH MODE (dễ hơn)
reader = DataReader(spark, source_type="kafka", kafka_mode="batch")

# STREAMING MODE (nâng cao)
reader = DataReader(spark, source_type="kafka", kafka_mode="streaming")
```

**Xong!** Code đã hỗ trợ cả 2 modes rồi! 🎉
