import pytest
from streamlit.testing.v1 import AppTest
import os
import sys
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, timedelta

# --- 1. SETUP PATH ---
current_test_dir = os.path.dirname(os.path.abspath(__file__))
dashboard_root_dir = os.path.dirname(current_test_dir)
if dashboard_root_dir not in sys.path:
    sys.path.insert(0, dashboard_root_dir)

APP_PATH = os.path.join(dashboard_root_dir, "app.py")

# --- 2. MOCK DATA GENERATORS ---
def get_mock_forecast_df():
    now = datetime.now()
    return pd.DataFrame([{
        'datetime': now + timedelta(hours=i),
        'actual_temp': 25.0, 'predicted_temp': 26.5,
        'actual_humidity': 80.0, 'predicted_humidity': 75.0,
        'actual_wind': 10.0, 'predicted_wind': 12.0,
        'predicted_desc': 'Partly Cloudy'
    } for i in range(10)])

def get_mock_redis_data():
    return {
        'temperature': 28.5, 'humidity': 70, 'pressure': 1012,
        'wind_speed': 15.5, 'precipitation_mm': 0.0,
        'weather_desc': 'clear', 'disaster_risk_score': 10,
        'datetime': datetime.now().isoformat()
    }

# --- 3. TEST CASE ---
@patch('connectors.redis_connector.RedisConnector')
@patch('connectors.postgres_connector.PostgresConnector')
def test_app_starts_with_mock_data(mock_pg_cls, mock_redis_cls):
    """
    Kiểm tra UI chạy đúng với dữ liệu giả lập và không bị treo vòng lặp
    """
    # Kích hoạt Test Mode để chặn st.rerun() trong app.py
    os.environ["STREAMLIT_TEST_MODE"] = "true"
    
    # Setup Redis Mock
    mock_redis = mock_redis_cls.return_value
    mock_redis.connect.return_value = True
    mock_redis.get_all_cities.return_value = ["Hanoi"]
    mock_redis.get_current_weather.return_value = get_mock_redis_data()
    mock_redis.is_connected.return_value = True
    
    # Setup Postgres Mock
    mock_pg = mock_pg_cls.return_value
    mock_pg.connect.return_value = True
    mock_pg.get_available_cities.return_value = ["Hanoi"]
    mock_pg.get_forecast.return_value = get_mock_forecast_df()
    mock_pg.is_connected.return_value = True

    # Khởi tạo và chạy AppTest
    at = AppTest.from_file(APP_PATH)
    at.run(timeout=10)

    # Dọn dẹp môi trường
    os.environ["STREAMLIT_TEST_MODE"] = "false"

    # Kiểm tra kết quả
    if at.exception:
        print(f"❌ App Crash Trace: {at.exception[0].stack_trace}")
        pytest.fail(f"App crashed: {at.exception[0].message}")

    assert not at.exception
    assert len(at.tabs) == 2
    print("✅ Smoke test PASSED with Mock Data and No Infinite Loop!")