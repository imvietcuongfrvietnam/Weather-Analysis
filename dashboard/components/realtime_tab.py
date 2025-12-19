"""
Real-Time Weather Tab
Tab hiển thị dữ liệu real-time từ Redis, auto-refresh 10s
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.redis_connector import RedisConnector
import config

def create_metric_cards(weather_data):
    """Create metric cards for current weather"""
    if not weather_data:
        st.warning("⚠️ No real-time data available from Redis")
        st.info("💡 Make sure ETL pipeline is running and writing to Redis")
        return
    
    # Extract data
    temp = weather_data.get('temp_celsius', 0)
    humidity = weather_data.get('humidity_pct', 0)
    pressure = weather_data.get('pressure_hpa', 0)
    wind = weather_data.get('wind_speed_kmh', 0)
    precip = weather_data.get('precipitation_mm', 0)
    condition = weather_data.get('weather_condition', 'unknown')
    risk = weather_data.get('disaster_risk_score', 0)
    
    # Display in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🌡️ Temperature",
            value=f"{temp:.1f}°C",
            delta=f"{temp * 1.8 + 32:.1f}°F"
        )
    
    with col2:
        st.metric(
            label="💧 Humidity",
            value=f"{humidity:.0f}%"
        )
    
    with col3:
        st.metric(
            label="🌪️ Pressure",
            value=f"{pressure:.0f} hPa"
        )
    
    with col4:
        st.metric(
            label="💨 Wind Speed",
            value=f"{wind:.1f} km/h"
        )
    
    # Second row
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric(
            label="🌧️ Precipitation",
            value=f"{precip:.1f} mm"
        )
    
    with col6:
        # Weather condition with icon
        condition_icons = {
            'clear': '☀️',
            'cloudy': '☁️',
            'rain': '🌧️',
            'snow': '❄️',
            'storm': '⛈️'
        }
        icon = condition_icons.get(condition, '🌤️')
        st.metric(
            label=f"{icon} Condition",
            value=condition.title()
        )
    
    with col7:
        # Risk score with color
        risk_level = "Low" if risk < 30 else "Medium" if risk < 60 else "High"
        risk_color = "🟢" if risk < 30 else "🟡" if risk < 60 else "🔴"
        st.metric(
            label=f"{risk_color} Risk Score",
            value=f"{risk:.0f}/100",
            delta=risk_level
        )
    
    with col8:
        # Last update time
        update_time = weather_data.get('datetime', datetime.now().isoformat())
        if isinstance(update_time, str):
            try:
                update_dt = datetime.fromisoformat(update_time.replace('Z', '+00:00'))
                time_str = update_dt.strftime("%H:%M:%S")
            except:
                time_str = "Unknown"
        else:
            time_str = update_time.strftime("%H:%M:%S")
        
        st.metric(
            label="⏰ Last Update",
            value=time_str
        )


def create_gauge_chart(value, title, max_value, color='blue'):
    """Create a gauge chart for a single metric"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title},
        gauge={
            'axis': {'range': [None, max_value]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, max_value * 0.33], 'color': "lightgray"},
                {'range': [max_value * 0.33, max_value * 0.66], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))
    
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    return fig


def show_realtime_tab():
    """Main function to show real-time weather tab"""
    
    st.header("🔥 Real-Time Weather Data")
    st.caption(f"Auto-refresh every {config.DASHBOARD_REFRESH_SECONDS} seconds")
    
    # City selector
    redis_conn = RedisConnector()
    
    # Try to connect
    if not redis_conn.connect():
        st.error("❌ Could not connect to Redis")
        st.info("💡 Make sure Redis server is running:")
        st.code("docker run -d -p 6379:6379 redis:latest")
        return
    
    # Get available cities
    cities = redis_conn.get_all_cities()
    
    if not cities:
        st.warning("⚠️  No cities found in Redis")
        st.info("💡 ETL pipeline needs to write data to Redis first")
        redis_conn.close()
        return
    
    # City selection
    selected_city = st.selectbox(
        "Select City",
        cities,
        index=0 if config.DEFAULT_CITY not in cities else cities.index(config.DEFAULT_CITY)
    )
    
    # Fetch current weather
    weather_data = redis_conn.get_current_weather(selected_city)
    
    if not weather_data:
        st.warning(f"⚠️ No data for {selected_city}")
        redis_conn.close()
        return
    
    # Display metric cards
    create_metric_cards(weather_data)
    
    st.divider()
    
    # Gauge charts row
    st.subheader("📊 Current Conditions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        temp = weather_data.get('temp_celsius', 0)
        fig_temp = create_gauge_chart(temp, "Temperature (°C)", 50, 'red')
        st.plotly_chart(fig_temp, use_container_width=True)
    
    with col2:
        humidity = weather_data.get('humidity_pct', 0)
        fig_humidity = create_gauge_chart(humidity, "Humidity (%)", 100, 'blue')
        st.plotly_chart(fig_humidity, use_container_width=True)
    
    with col3:
        wind = weather_data.get('wind_speed_kmh', 0)
        fig_wind = create_gauge_chart(wind, "Wind Speed (km/h)", 100, 'green')
        st.plotly_chart(fig_wind, use_container_width=True)
    
    # Connection status
    st.divider()
    if redis_conn.is_connected():
        st.success(f"✅ Connected to Redis ({config.REDIS_HOST}:{config.REDIS_PORT})")
    else:
        st.error("❌ Redis connection lost")
    
    redis_conn.close()


if __name__ == "__main__":
    # For testing
    st.set_page_config(page_title="Real-Time Weather", layout="wide")
    show_realtime_tab()
