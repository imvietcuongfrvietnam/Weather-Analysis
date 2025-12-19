# Spark ETL - Weather & Disaster Prediction NYC

## Project Overview

Hệ thống ETL xử lý 4 nguồn dữ liệu:

1. Weather Data (Kaggle - Historical Hourly Weather)
2. 311 Service Requests (NYC Open Data)
3. NYC Taxi Trip Records (TLC)
4. Motor Vehicle Collisions (NYC)

## Mục tiêu

- Clean, normalize, enrich dữ liệu từ 4 nguồn
- Tích hợp dữ liệu để phân tích mối liên hệ weather-disaster-traffic
- Chuẩn bị data cho ML prediction models

## Project Structure

```
spark_etl_weather_disaster/
├── config/              # Configurations
├── schemas/             # Data schemas cho 4 nguồn
├── readers/             # Đọc data (fake cho testing)
├── transformations/     # Clean, normalize, enrich
│   ├── cleaning.py
│   ├── normalization.py
│   └── enrichment.py
├── writers/             # Ghi data (fake cho testing)
├── utils/               # Helper functions
├── tests/               # Unit tests
├── data_samples/        # Sample data cho testing
└── main_etl.py          # Main pipeline
```

## Setup

```bash
# Install dependencies
pip install pyspark pandas numpy

# Run ETL
python main_etl.py
```

## Data Sources

- Weather: https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data
- 311 Requests: NYC Open Data
- Taxi Trips: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Collisions: NYC Open Data
