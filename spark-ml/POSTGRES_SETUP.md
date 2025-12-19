# PostgreSQL Integration - Weather Forecasting

## ✅ Đã thêm vào hệ thống

Hệ thống Weather Forecasting đã được tích hợp PostgreSQL writer. Sau khi dự đoán xong, dữ liệu sẽ tự động ghi vào PostgreSQL database.

## 📦 Files mới

1. **`postgres_writer.py`** - Module ghi dữ liệu vào PostgreSQL
2. **`.env.example`** - Template cho environment variables

## ⚙️ Cấu hình

### Config trong `config.py`

Đã thêm các biến môi trường để cấu hình PostgreSQL:

```python
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_forecast")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "weather_predictions")
POSTGRES_WRITE_MODE = os.getenv("POSTGRES_WRITE_MODE", "append")
```

### Thay đổi credentials

**Cách 1: Environment Variables (Khuyến nghị)**
```bash
export POSTGRES_HOST=your-postgres-host
export POSTGRES_PORT=5432
export POSTGRES_DB=weather_forecast
export POSTGRES_USER=your_username
export POSTGRES_PASSWORD=your_password
```

**Cách 2: File .env**
```bash
# Copy example file
cp .env.example .env

# Edit file .env với credentials thật
nano .env

# Load environment variables
export $(cat .env | xargs)
```

**Cách 3: Docker Compose**
```yaml
environment:
  - POSTGRES_HOST=weather-postgres
  - POSTGRES_PORT=5432
  - POSTGRES_DB=weather_forecast
  - POSTGRES_USER=postgres
  - POSTGRES_PASSWORD=mypassword
```

## 🐘 Setup PostgreSQL Server

### Option 1: Docker (Khuyến nghị - dễ nhất)

```bash
# Chạy PostgreSQL container
docker run -d \
  --name weather-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=weather_forecast \
  -p 5432:5432 \
  postgres:15

# Check status
docker ps | grep weather-postgres
```

### Option 2: Local Installation

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql
```

**macOS:**
```bash
brew install postgresql@15
brew services start postgresql@15
```

### Create Database & Table

```bash
# Print SQL schema
python postgres_writer.py

# Hoặc tự động tạo:
# 1. Connect to PostgreSQL
psql -U postgres -h localhost

# 2. Chạy commands sau trong psql:
CREATE DATABASE weather_forecast;
\c weather_forecast

CREATE TABLE IF NOT EXISTS weather_predictions (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    city VARCHAR(100),
    
    -- Actual values
    temp_celsius DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    weather_condition VARCHAR(50),
    
    -- Predicted values
    prediction_temp_celsius DOUBLE PRECISION,
    prediction_humidity_pct DOUBLE PRECISION,
    prediction_pressure_hpa DOUBLE PRECISION,
    prediction_wind_speed_kmh DOUBLE PRECISION,
    prediction_precipitation_mm DOUBLE PRECISION,
    prediction_weather_condition VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_datetime ON weather_predictions(datetime);
CREATE INDEX idx_city ON weather_predictions(city);

\q
```

## 🧪 Test Connection

```bash
cd /home/leminhtu/Weather-Analysis/spark-ml

# Xem setup instructions và test
python postgres_writer.py
```

## 🚀 Sử dụng

### Chạy forecasting với PostgreSQL

```bash
# Sau khi setup PostgreSQL, chạy như bình thường
python weather_forecasting.py
```

Pipeline sẽ tự động:
1. Load data từ MinIO
2. Train models
3. Predict
4. **Ghi vào PostgreSQL** (Step 9)
5. Export CSV
6. Create plots

### Nếu PostgreSQL chưa setup

Hệ thống sẽ vẫn chạy bình thường và:
- ⚠️ Warning: "Could not write to PostgreSQL"
- ✅ Vẫn save CSV và plots như bình thường
- 💡 Hiển thị hướng dẫn setup

**Không có lỗi, không dừng pipeline!**

## 📊 Truy vấn dữ liệu

Sau khi có data trong PostgreSQL:

```sql
-- Xem tất cả predictions
SELECT * FROM weather_predictions 
ORDER BY datetime DESC 
LIMIT 10;

-- So sánh actual vs predicted temperature
SELECT 
    datetime,
    city,
    temp_celsius as actual_temp,
    prediction_temp_celsius as predicted_temp,
    ABS(temp_celsius - prediction_temp_celsius) as error
FROM weather_predictions
WHERE temp_celsius IS NOT NULL
ORDER BY datetime DESC;

-- Tính MAE trung bình
SELECT 
    AVG(ABS(temp_celsius - prediction_temp_celsius)) as mae_temp,
    AVG(ABS(humidity_pct - prediction_humidity_pct)) as mae_humidity,
    AVG(ABS(pressure_hpa - prediction_pressure_hpa)) as mae_pressure
FROM weather_predictions
WHERE prediction_temp_celsius IS NOT NULL;

-- Predictions by city
SELECT city, COUNT(*) as prediction_count
FROM weather_predictions
GROUP BY city;

-- Latest predictions
SELECT datetime, city, 
       prediction_temp_celsius, 
       prediction_weather_condition
FROM weather_predictions
WHERE created_at >= NOW() - INTERVAL '1 day'
ORDER BY datetime DESC;
```

## 🔄 Data Flow

```
MinIO → Spark ML → PostgreSQL
                ↓
              CSV + Plots
```

## 📁 Schema Table

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| datetime | TIMESTAMP | Thời điểm dự đoán |
| city | VARCHAR(100) | Thành phố |
| temp_celsius | DOUBLE | Nhiệt độ thực tế |
| prediction_temp_celsius | DOUBLE | Nhiệt độ dự đoán |
| humidity_pct | DOUBLE | Độ ẩm thực tế |
| prediction_humidity_pct | DOUBLE | Độ ẩm dự đoán |
| pressure_hpa | DOUBLE | Áp suất thực tế |
| prediction_pressure_hpa | DOUBLE | Áp suất dự đoán |
| wind_speed_kmh | DOUBLE | Tốc độ gió thực tế |
| prediction_wind_speed_kmh | DOUBLE | Tốc độ gió dự đoán |
| precipitation_mm | DOUBLE | Lượng mưa thực tế |
| prediction_precipitation_mm | DOUBLE | Lượng mưa dự đoán |
| weather_condition | VARCHAR(50) | Tình trạng thực tế |
| prediction_weather_condition | VARCHAR(50) | Tình trạng dự đoán |
| created_at | TIMESTAMP | Thời điểm ghi vào DB |

## 🔒 Security

**Production deployment:**
1. Đừng commit credentials vào git
2. Dùng environment variables
3. Set strong password
4. Limit network access (firewall)
5. Enable SSL connection nếu remote

## 🛠️ Troubleshooting

### Lỗi: "Could not write to PostgreSQL"

**Nguyên nhân:** PostgreSQL server không chạy hoặc credentials sai

**Giải pháp:**
```bash
# Check PostgreSQL running
docker ps | grep postgres
# hoặc
sudo systemctl status postgresql

# Test connection
psql -U postgres -h localhost -d weather_forecast

# Check credentials in environment
env | grep POSTGRES
```

### Lỗi: "Relation does not exist"

**Nguyên nhân:** Table chưa được tạo

**Giải pháp:**
```bash
python postgres_writer.py  # Xem SQL schema
# Copy và chạy trong psql
```

### Lỗi: JDBC driver not found

**Nguyên nhân:** PostgreSQL JDBC driver chưa được tải

**Giải pháp:** Spark sẽ tự động tải khi chạy lần đầu. Cần internet connection.

## 💡 Tips

1. **Development**: Dùng Docker PostgreSQL (dễ setup/teardown)
2. **Production**: Dùng managed PostgreSQL (AWS RDS, Azure Database, etc.)
3. **Backup**: Set up automated backups cho production
4. **Monitoring**: Monitor table size và query performance
5. **Indexes**: Thêm indexes cho columns thường query

## ✅ Checklist Setup

- [ ] Install PostgreSQL server (Docker hoặc local)
- [ ] Create database `weather_forecast`
- [ ] Create table `weather_predictions`
- [ ] Set environment variables với credentials thật
- [ ] Test connection: `python postgres_writer.py`
- [ ] Run forecast: `python weather_forecasting.py`
- [ ] Verify data: `SELECT COUNT(*) FROM weather_predictions;`

---

**Hệ thống đã sẵn sàng! Chỉ cần setup PostgreSQL và thay credentials là chạy được ngay! 🚀**
