# Weather Forecasting ML System

Hệ thống dự đoán thời tiết 7 ngày sử dụng Spark ML và dữ liệu từ MinIO.

## Tính năng

Dự đoán các đặc trưng thời tiết:
- **Nhiệt độ** (°C, °F)
- **Độ ẩm** (%)
- **Áp suất khí quyển** (hPa)
- **Tốc độ gió** (km/h)
- **Lượng mưa** (mm)
- **Tình trạng thời tiết** (clear/cloudy/rain/snow/storm)

## Cấu trúc Project

```
spark-ml/
├── weather_forecasting.py      # Main pipeline
├── config.py                   # Configuration
├── data_loader.py             # Load data từ MinIO
├── feature_engineering.py     # Time series features
├── models.py                  # ML models
├── forecast_evaluator.py      # Evaluation metrics
├── visualization.py           # Plotting
├── forecasts/                 # Output forecasts (CSV)
├── saved_models/              # Trained models
└── forecasts/plots/           # Visualization plots
```

## Yêu cầu

```bash
pip install pyspark pandas matplotlib
```

## Chạy Forecasting

### Cơ bản:
```bash
cd /home/leminhtu/Weather-Analysis/spark-ml
python weather_forecasting.py
```

### Với tùy chọn:
```bash
# Giới hạn số dòng (testing)
python weather_forecasting.py --limit 1000

# Chọn thành phố
python weather_forecasting.py --city "New York"

# Không lưu models
python weather_forecasting.py --no-save-models

# Không tạo plots
python weather_forecasting.py --no-plots
```

## Output

### 1. Predictions (CSV)
Lưu tại: `forecasts/weather_forecast_YYYYMMDD_HHMMSS.csv`

Các cột:
- datetime
- city
- temp_celsius, prediction_temp_celsius
- humidity_pct, prediction_humidity_pct
- pressure_hpa, prediction_pressure_hpa
- wind_speed_kmh, prediction_wind_speed_kmh
- precipitation_mm, prediction_precipitation_mm
- weather_condition, prediction_weather_condition

### 2. Models
Lưu tại: `saved_models/<feature>_model/`

Có thể load lại để dự đoán mà không cần train lại.

### 3. Plots
Lưu tại: `forecasts/plots/`

- `<feature>_forecast.png` - Từng đặc trưng
- `forecast_dashboard.png` - Dashboard tổng hợp
- `metrics_comparison.png` - So sánh hiệu suất models

## Cách hoạt động

1. **Load Data**: Đọc dữ liệu enriched từ MinIO (`s3a://weather-data/enriched/weather/`)
2. **Feature Engineering**: 
   - Lag features (past values)
   - Rolling statistics (mean, std, sum, max)
   - Time features (hour, day, month, cyclical encoding)
   - Derived features (heat index, wind chill, etc.)
3. **Train Models**:
   - GBT/Random Forest cho regression
   - Random Forest Classifier cho weather condition
4. **Evaluate**: MAE, RMSE, R², MAPE, F1, Accuracy
5. **Export**: CSV + Plots

## Model Performance

Models được đánh giá bằng:
- **Regression**: MAE, RMSE, R², MAPE
- **Classification**: Accuracy, F1, Precision, Recall

## Lưu ý

- Cần ít nhất 100 records để train
- Khuyến nghị 14-30+ ngày dữ liệu lịch sử
- MinIO phải đang chạy và có dữ liệu từ ETL

## Troubleshooting

### Lỗi: "Could not connect to MinIO"
- Kiểm tra MinIO đang chạy: `docker ps`
- Kiểm tra MINIO_ENDPOINT trong `config.py`

### Lỗi: "Insufficient data"
- Chạy ETL pipeline trước để tạo dữ liệu trong MinIO
- Hoặc dùng `--limit` để test với ít data hơn

### Lỗi: S3A dependencies
- Spark sẽ tự động tải packages cần thiết
- Cần internet connection lần đầu chạy
