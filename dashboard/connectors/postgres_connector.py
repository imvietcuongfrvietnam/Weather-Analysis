"""
PostgreSQL Connector
Kết nối và lấy dữ liệu dự báo từ PostgreSQL
"""

import psycopg2
from psycopg2 import pool
import pandas as pd
import sys
import os

# --- SETUP IMPORT CONFIG ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import config
except ImportError:
    # Fallback
    class Config:
        POSTGRES_HOST = "localhost"
        POSTGRES_PORT = "5432"
        POSTGRES_DB = "weather_forecast"
        POSTGRES_USER = "postgres"
        POSTGRES_PASSWORD = "password"
        POSTGRES_TABLE = "weather_predictions"
    config = Config()

class PostgresConnector:
    """Connect to PostgreSQL and fetch forecast data"""
    
    def __init__(self):
        self.host = config.POSTGRES_HOST
        self.port = config.POSTGRES_PORT
        self.database = config.POSTGRES_DB
        self.user = config.POSTGRES_USER
        self.password = config.POSTGRES_PASSWORD
        self.table = config.POSTGRES_TABLE
        self.connection_pool = None
        
    def connect(self) -> bool:
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            return True
        except Exception as e:
            print(f"❌ PostgreSQL connection error: {e}")
            return False

def create_forecast_table(df):
    """Create forecast summary table"""
    
    # Group by date (aggregate hourly to daily)
    df['date'] = pd.to_datetime(df['datetime']).dt.date
    
    daily = df.groupby('date').agg({
        'prediction_temp_celsius': 'mean',
        'prediction_humidity_pct': 'mean',
        'prediction_pressure_hpa': 'mean',
        'prediction_wind_speed_kmh': 'mean',
        'prediction_precipitation_mm': 'sum',
        'prediction_weather_condition': lambda x: x.mode()[0] if len(x) > 0 else 'unknown'
    }).reset_index()
    
    # Rename columns
    daily.columns = ['Date', 'Temp (°C)', 'Humidity (%)', 'Pressure (hPa)', 
                     'Wind (km/h)', 'Precip (mm)', 'Condition']
    
    # Round values
    daily['Temp (°C)'] = daily['Temp (°C)'].round(1)
    daily['Humidity (%)'] = daily['Humidity (%)'].round(0)
    daily['Pressure (hPa)'] = daily['Pressure (hPa)'].round(0)
    daily['Wind (km/h)'] = daily['Wind (km/h)'].round(1)
    daily['Precip (mm)'] = daily['Precip (mm)'].round(1)
    
    return daily


def show_accuracy_metrics(df):
    """Show prediction accuracy metrics"""
    
    st.subheader("📈 Model Accuracy")
    
    # Calculate MAE for each feature
    metrics = {}
    
    features = [
        ('temp_celsius', 'prediction_temp_celsius', 'Temperature', '°C'),
        ('humidity_pct', 'prediction_humidity_pct', 'Humidity', '%'),
        ('pressure_hpa', 'prediction_pressure_hpa', 'Pressure', 'hPa'),
        ('wind_speed_kmh', 'prediction_wind_speed_kmh', 'Wind Speed', 'km/h'),
        ('precipitation_mm', 'prediction_precipitation_mm', 'Precipitation', 'mm')
    ]
    
    col1, col2, col3, col4, col5 = st.columns(5)
    cols = [col1, col2, col3, col4, col5]
    
    for idx, (actual_col, pred_col, name, unit) in enumerate(features):
        if actual_col in df.columns and pred_col in df.columns:
            # Filter non-null values
            mask = df[actual_col].notna() & df[pred_col].notna()
            if mask.any():
                mae = (df.loc[mask, actual_col] - df.loc[mask, pred_col]).abs().mean()
                
                with cols[idx]:
                    st.metric(
                        label=f"{name} MAE",
                        value=f"{mae:.2f} {unit}"
                    )


def show_forecast_tab():
    """Main function to show forecast tab"""
    
    st.header("📊 7-Day Weather Forecast")
    st.caption("Predictions from ML models")
    
    # PostgreSQL connector
    pg_conn = PostgresConnector()
    
    # Try to connect
    if not pg_conn.connect():
        st.error("❌ Could not connect to PostgreSQL")
        st.info("💡 Make sure PostgreSQL server is running and configured")
        st.code("""
# Start PostgreSQL with Docker
docker run -d \\
  --name weather-postgres \\
  -e POSTGRES_PASSWORD=postgres \\
  -e POSTGRES_DB=weather_forecast \\
  -p 5432:5432 \\
  postgres:15
        """)
        return
    
    # Get available cities
    cities = pg_conn.get_available_cities()
    
    if not cities:
        st.warning("⚠️  No forecast data in PostgreSQL yet")
        st.info("💡 Run ML forecasting pipeline first:")
        st.code("cd spark-ml && python weather_forecasting.py")
        pg_conn.close()
        return
    
    # City selection
    selected_city = st.selectbox(
        "Select City",
        cities,
        index=0 if config.DEFAULT_CITY not in cities else cities.index(config.DEFAULT_CITY),
        key="forecast_city"
    )
    
    # Fetch forecast data
    df = pg_conn.get_latest_predictions(selected_city, limit=168)
    
    if df is None or df.empty:
        st.warning(f"⚠️ No forecast data for {selected_city}")
        pg_conn.close()
        return
    
    # Show sample count
    st.info(f"📊 Showing {len(df)} predictions for {selected_city}")
    
    # Show accuracy metrics (if we have actual values)
    if 'temp_celsius' in df.columns and df['temp_celsius'].notna().any():
        show_accuracy_metrics(df)
        st.divider()
    
    # Forecast charts
    st.subheader("📈 Forecast Charts")
    
    # Temperature
    if 'prediction_temp_celsius' in df.columns:
        fig_temp = create_forecast_chart(
            df, "Temperature", 
            'prediction_temp_celsius', 
            'temp_celsius', 
            "°C"
        )
        st.plotly_chart(fig_temp, use_container_width=True)
    
    # Humidity & Pressure side by side
    col1, col2 = st.columns(2)
    
    with col1:
        if 'prediction_humidity_pct' in df.columns:
            fig_humidity = create_forecast_chart(
                df, "Humidity",
                'prediction_humidity_pct',
                'humidity_pct',
                "%"
            )
            st.plotly_chart(fig_humidity, use_container_width=True)
    
    with col2:
        if 'prediction_pressure_hpa' in df.columns:
            fig_pressure = create_forecast_chart(
                df, "Pressure",
                'prediction_pressure_hpa',
                'pressure_hpa',
                "hPa"
            )
            st.plotly_chart(fig_pressure, use_container_width=True)
    
    # Wind & Precipitation side by side
    col3, col4 = st.columns(2)
    
    with col3:
        if 'prediction_wind_speed_kmh' in df.columns:
            fig_wind = create_forecast_chart(
                df, "Wind Speed",
                'prediction_wind_speed_kmh',
                'wind_speed_kmh',
                "km/h"
            )
            st.plotly_chart(fig_wind, use_container_width=True)
    
    with col4:
        if 'prediction_precipitation_mm' in df.columns:
            fig_precip = create_forecast_chart(
                df, "Precipitation",
                'prediction_precipitation_mm',
                'precipitation_mm',
                "mm"
            )
            st.plotly_chart(fig_precip, use_container_width=True)
    
    st.divider()
    
    # Forecast table
    st.subheader("📋 Daily Forecast Summary")
    daily_forecast = create_forecast_table(df)
    st.dataframe(daily_forecast, use_container_width=True, hide_index=True)
    
    # CSV download
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Full Forecast (CSV)",
        data=csv,
        file_name=f"weather_forecast_{selected_city}.csv",
        mime="text/csv"
    )
    
    # Connection status
    st.divider()
    if pg_conn.is_connected():
        st.success(f"✅ Connected to PostgreSQL ({config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB})")
    else:
        st.error("❌ PostgreSQL connection lost")
    
    pg_conn.close()

if __name__ == "__main__":
    print(f"🧪 Testing Postgres connection to {config.POSTGRES_HOST}:{config.POSTGRES_PORT}...")
    db = PostgresConnector()
    if db.connect():
        print("✅ Connected!")
    else:
        print("❌ Failed.")