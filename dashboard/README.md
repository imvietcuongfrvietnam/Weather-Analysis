# Weather Forecast Dashboard

Real-time weather dashboard với dữ liệu từ Redis và PostgreSQL.

## ✨ Features

### Tab 1: Real-Time Weather 🔥
- **Data source**: Redis
- **Refresh**: Auto 10 giây
- **Display**:
  - Metric cards (temp, humidity, pressure, wind, precipitation)
  - Gauge charts cho các chỉ số quan trọng
  - Weather condition & risk score
  - Last update timestamp

### Tab 2: 7-Day Forecast 📊
- **Data source**: PostgreSQL
- **Display**:
  - Interactive comparison charts (actual vs predicted)
  - 7-day forecast table
  - Model accuracy metrics (MAE)
  - CSV download

## 🚀 Quick Start

### 1. Install Dependencies
```bash
cd dashboard
pip install -r requirements.txt
```

### 2. Configure
```bash
# Copy environment template
cp .env.example .env

# Edit .env với credentials thật
nano .env
```

### 3. Run Dashboard
```bash
streamlit run app.py
```

Dashboard sẽ mở tại: http://localhost:8501

## 📋 Prerequisites

### Required Services Running:

1. **Redis** (for real-time data)
```bash
docker run -d --name redis -p 6379:6379 redis:latest
```

2. **PostgreSQL** (for forecasts)
```bash
docker run -d --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=weather_forecast \
  -p 5432:5432 \
  postgres:15
```

3. **ETL Pipeline** (writing to Redis)
```bash
cd ../spark_etl_weather_disaster
python main_etl.py
```

4. **ML Pipeline** (writing to PostgreSQL)
```bash
cd ../spark-ml
python weather_forecasting.py
```

## 📁 Project Structure

```
dashboard/
├── app.py                      # Main Streamlit app
├── config.py                   # Configuration settings
├── connectors/
│   ├── redis_connector.py      # Redis connection & data fetching
│   └── postgres_connector.py   # PostgreSQL connection & queries
├── components/
│   ├── realtime_tab.py         # Real-time weather tab
│   └── forecast_tab.py         # 7-day forecast tab
├── requirements.txt            # Python dependencies
├── .env.example                # Environment template
└── README.md                   # This file
```

## ⚙️ Configuration

### Environment Variables

Edit `.env` file:

```bash
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=weather_forecast
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_TABLE=weather_predictions

# Dashboard
DASHBOARD_REFRESH_SECONDS=10
DEFAULT_CITY=New York
```

### Custom Port

```bash
streamlit run app.py --server.port 8502
```

## 🧪 Testing Connections

### Test Redis
```bash
cd connectors
python redis_connector.py
```

### Test PostgreSQL
```bash
cd connectors
python postgres_connector.py
```

## 🎨 Features Detail

### Auto-Refresh
- Real-time tab tự động refresh mỗi 10 giây
- Hiển thị countdown timer
- Smooth updates không flickering

### Interactive Charts
- Plotly charts với zoom, pan, hover
- Comparison của actual vs predicted
- Multi-line charts cho trends

### Error Handling
- Graceful degradation khi service unavailable
- Clear error messages với hướng dẫn fix
- Connection status indicators

### Responsive Design
- Works on desktop, tablet, mobile
- Adaptive layout
- Clean, modern UI

## 🐳 Docker Deployment

### Build Image
```bash
docker build -t weather-dashboard .
```

### Run Container
```bash
docker run -d \
  -p 8501:8501 \
  -e REDIS_HOST=redis \
  -e POSTGRES_HOST=postgres \
  --name weather-dashboard \
  weather-dashboard
```

### Docker Compose
```yaml
version: '3.8'
services:
  dashboard:
    build: .
    ports:
      - "8501:8501"
    environment:
      REDIS_HOST: redis
      POSTGRES_HOST: postgres
    depends_on:
      - redis
      - postgres
  
  redis:
    image: redis:latest
    ports:
      - "6379:6379"
  
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: weather_forecast
    ports:
      - "5432:5432"
```

## 🔧 Troubleshooting

### Dashboard won't start
```bash
# Check dependencies
pip install -r requirements.txt

# Check Python version (needs 3.8+)
python --version
```

### Redis connection failed
```bash
# Check Redis running
docker ps | grep redis

# Test connection
redis-cli ping
```

### PostreSQL connection failed
```bash
# Check PostgreSQL running
docker ps | grep postgres

# Test connection
psql -h localhost -U postgres -d weather_forecast
```

### No data showing
```bash
# Make sure ETL piepeline is running (for Redis data)
cd ../spark_etl_weather_disaster
python main_etl.py

# Make sure ML pipeline ran (for PostgreSQL data)
cd ../spark-ml
python weather_forecasting.py
```

## 📊 Data Flow

```
Kafka → Spark ETL → Redis → Dashboard (Real-time tab)
                  ↓
                MinIO → Spark ML → PostgreSQL → Dashboard (Forecast tab)
```

## 🎯 Next Steps

- [ ] Add more cities
- [ ] Historical data analysis
- [ ] Alert notifications
- [ ] Export reports
- [ ] Mobile app

## 📝 Notes

- Real-time tab chỉ hiển thị data mới nhất từ Redis
- Forecast tab hiển thị 168 hours (7 days) predictions
- Auto-refresh chỉ áp dụng cho real-time tab
- Forecast tab refresh khi chuyển tab hoặc click Force Refresh

## 🌐 Deployment

### Streamlit Cloud
1. Push code to GitHub
2. Connect repo to streamlit.io
3. Set environment variables in Settings
4. Deploy!

### Heroku
```bash
heroku create weather-dashboard
heroku config:set REDIS_HOST=your-redis-url
heroku config:set POSTGRES_HOST=your-postgres-url
git push heroku main
```

---
### Cac cau lenh de chay dashboard
Dam bao da khoi tao dashboard trong minikube
chay cau lenh sau de expose dashboard giup truy cap tu may client:
kubectl port-forward svc/weather-dashboard 8501:80 -n default

**Dashboard hoàn chỉnh và sẵn sàng sử dụng! 🚀**