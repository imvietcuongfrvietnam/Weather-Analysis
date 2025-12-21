# Weather Forecasting ML - File Structure

## 📁 Clean Directory Structure

```
spark-ml/
├── Core Modules (Python)
│   ├── config.py                    # Configuration (MinIO + PostgreSQL)
│   ├── data_loader.py               # Load data from MinIO
│   ├── feature_engineering.py       # Time series features
│   ├── models.py                    # ML models (GBT + RF)
│   ├── forecast_evaluator.py        # Metrics and evaluation
│   ├── visualization.py             # Plots and dashboards
│   ├── postgres_writer.py           # Write to PostgreSQL
│   └── weather_forecasting.py       # MAIN PIPELINE
│
├── Testing & Setup
│   └── test_system.py               # Test MinIO connection
│
├── Documentation
│   ├── README.md                    # Usage guide
│   ├── POSTGRES_SETUP.md            # PostgreSQL setup
│   └── .env.example                 # Config template
│
├── Dependencies
│   └── requirements.txt             # Python packages
│
└── Output (auto-created)
    ├── forecasts/                   # CSV predictions
    │   └── plots/                   # Visualizations
    └── saved_models/                # Trained models
```

## ✅ All Files Are Essential

**No redundant files!** All 13 files serve specific purposes:

- **8 Python modules** - Core ML system
- **1 Test script** - System validation
- **3 Documentation** - Setup & usage
- **1 Requirements** - Dependencies

**Old files removed:**
- ❌ `spark_ml.py` (old HDFS classifier)
- ❌ `simul_spark_ml.py` (old mock example)

**System is clean and production-ready! 🚀**
