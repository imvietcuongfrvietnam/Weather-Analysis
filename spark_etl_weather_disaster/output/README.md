# 📊 Output Directory - Cleaned Data

This directory contains cleaned and processed data in JSON format, ready for analysis or further processing.

## 📁 Generated Files

### 1. **Cleaned Data (After STEP 2: CLEANING)**

| File                        | Records | Size    | Description                                                        |
| --------------------------- | ------- | ------- | ------------------------------------------------------------------ |
| `weather_cleaned.json`      | 1000    | ~333 KB | Weather data with validated temperature, humidity, pressure ranges |
| `311_requests_cleaned.json` | 500     | ~316 KB | NYC 311 service requests with validated coordinates                |
| `taxi_trips_cleaned.json`   | 800     | ~454 KB | Taxi trips with validated fares, distances, passenger counts       |
| `collisions_cleaned.json`   | 300     | ~252 KB | Collision data with validated injury/death counts                  |

### 2. **Final Integrated Data (After STEP 5: ENRICHMENT)**

| File                            | Records | Size    | Description                                                                  |
| ------------------------------- | ------- | ------- | ---------------------------------------------------------------------------- |
| `integrated_final_cleaned.json` | 1000    | ~1.1 MB | **Complete dataset** with all transformations and enrichments (38+ features) |

---

## 🔍 Data Processing Steps

```
┌─────────────────┐
│  Raw CSV Data   │  (./data/*.csv - generated once)
│  1000+ records  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  STEP 2: CLEAN  │  Type casting, null removal, validation
│  Data Quality   │  → weather_cleaned.json (333 KB)
└────────┬────────┘  → 311_requests_cleaned.json (316 KB)
         │           → taxi_trips_cleaned.json (454 KB)
         ▼           → collisions_cleaned.json (252 KB)
┌─────────────────┐
│ STEP 3: NORMAL  │  Unit conversion (Kelvin→Celsius, m/s→km/h)
│ Standardization │  Datetime normalization, text formatting
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ STEP 4: ENRICH  │  Disaster risk scoring
│ Feature Engineer│  Traffic impact analysis
└────────┬────────┘  Join weather + taxi + collision data
         │
         ▼
┌─────────────────┐
│  STEP 5: FINAL  │  → integrated_final_cleaned.json (1.1 MB)
│  Integrated     │  38+ features including:
│  Dataset        │  - Disaster risk scores
└─────────────────┘  - Traffic impact scores
                     - Weather comfort index
                     - ML-ready features
```

---

## 📋 Data Schema

### Weather Cleaned JSON

```json
{
  "datetime": "2016-01-01 00:00:00",
  "city": "New York",
  "temperature": 280.51,
  "humidity": 38.17,
  "pressure": 1008.75,
  "wind_speed": 8.3,
  "wind_direction": "51.71",
  "weather_description": "snow",
  "rain_1h": 0.0,
  "snow_1h": 0.0,
  "clouds_all": "4"
}
```

### Integrated Final JSON (Sample)

```json
{
  "datetime": "2016-01-01 00:00:00",
  "city": "New York",
  "temperature": 280.51,
  "temp_celsius": 7.36,
  "temp_fahrenheit": 45.26,
  "weather_condition": "snow",
  "disaster_risk_score": 20,
  "emergency_level": "low",
  "trip_count": 0,
  "collision_count": 0,
  "traffic_impact_score": 30,
  "is_weekend": 0,
  "is_rush_hour": 0,
  "season": "winter",
  "weather_comfort_index": 53.88,
  "data_quality_score": 100,
  ...
}
```

---

## 🎯 Use Cases

### For Data Scientists:

- **ML Training**: Use `integrated_final_cleaned.json` for model training
- **Feature Analysis**: Explore engineered features like disaster risk, traffic impact
- **Data Quality**: All data validated, no nulls in critical fields

### For Analysts:

- **Quick Inspection**: Open JSON files in any text editor or JSON viewer
- **Statistics**: Already cleaned data, ready for aggregation
- **Visualization**: Import into Tableau, Power BI, or Python/R

### For Developers:

- **API Integration**: JSON format ready for REST APIs
- **Database Import**: Load into MongoDB, PostgreSQL (JSONB), or Elasticsearch
- **Testing**: Use cleaned data for unit tests and integration tests

---

## 🔄 Migration Path

### Current (Development):

```
CSV files → Spark ETL → JSON files (./output/)
```

### Future (Production):

```
Kafka streams → Spark ETL → HDFS (distributed storage)
                         → Elasticsearch (search & analytics)
```

**To switch output destination**, edit `main_etl.py`:

```python
# Change from:
data_writer = DataWriter(output_type="json")

# To HDFS:
data_writer = DataWriter(output_type="hdfs")

# Or Elasticsearch:
data_writer = DataWriter(output_type="elasticsearch")
```

---

## 📝 Notes

- **Files are auto-generated** by `main_etl.py`
- **Not version controlled** (excluded in `.gitignore`)
- **Regenerate anytime** by running: `py -3.11 main_etl.py`
- **File format**: JSON with `orient='records'` for easy reading

---

## 🚀 Quick Commands

```bash
# View weather cleaned data
cat weather_cleaned.json | head -50

# Count records using jq (if installed)
jq '. | length' weather_cleaned.json

# Open in VS Code
code integrated_final_cleaned.json

# Open in browser (pretty print)
start weather_cleaned.json
```

---

**Generated by:** Spark ETL Pipeline v1.0  
**Last Updated:** 2025-12-02
