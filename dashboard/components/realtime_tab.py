"""
Real-Time Weather Tab
Hiển thị dữ liệu từ Redis với các tên cột đã chuẩn hóa
"""

import streamlit as st
import plotly.graph_objects as go
from datetime import datetime
import sys
import os

# Setup path để import connectors và config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.redis_connector import RedisConnector
import config

def create_metric_cards(weather_data):
    """Tạo các thẻ thông số cho thời tiết hiện tại"""
    if not weather_data:
        st.warning("⚠️ No real-time data available from Redis")
        return
    
    # Lấy dữ liệu theo tên cột chuẩn mới (Khớp với ETL pipeline)
    temp = weather_data.get('temperature', 0)
    humidity = weather_data.get('humidity', 0)
    pressure = weather_data.get('pressure', 0)
    wind = weather_data.get('wind_speed', 0)
    precip = weather_data.get('precipitation_mm', 0)
    condition = weather_data.get('weather_desc', 'unknown')
    risk = weather_data.get('disaster_risk_score', 0)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="🌡️ Temperature", value=f"{temp:.1f}°C")
    with col2:
        st.metric(label="💧 Humidity", value=f"{humidity:.0f}%")
    with col3:
        st.metric(label="🌪️ Pressure", value=f"{pressure:.0f} hPa")
    with col4:
        st.metric(label="💨 Wind Speed", value=f"{wind:.1f} km/h")
    
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric(label="🌧️ Precipitation", value=f"{precip:.1f} mm")
    with col6:
        st.metric(label="🌤️ Condition", value=condition.title())
    with col7:
        risk_level = "Low" if risk < 30 else "Medium" if risk < 60 else "High"
        st.metric(label="⚠️ Risk Score", value=f"{risk:.0f}/100", delta=risk_level)
    with col8:
        # Xử lý thời gian hiển thị
        update_time = weather_data.get('datetime', datetime.now().isoformat())
        time_display = update_time[-8:] if len(update_time) > 8 else "Just now"
        st.metric(label="⏰ Last Update", value=time_display)

def create_gauge_chart(value, title, max_value, color='blue'):
    """Tạo biểu đồ đồng hồ đo bằng Plotly"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title},
        gauge={'axis': {'range': [None, max_value]}, 'bar': {'color': color}}
    ))
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    return fig

def show_realtime_tab():
    """Hàm chính hiển thị tab Real-time"""
    st.header("🔥 Real-Time Weather Data")
    
    redis_conn = RedisConnector()
    if not redis_conn.connect():
        st.error("❌ Could not connect to Redis")
        return
    
    cities = redis_conn.get_all_cities()
    if not cities:
        st.warning("⚠️ No cities found in Redis. Run ETL pipeline first.")
        redis_conn.close()
        return
    
    selected_city = st.selectbox("Select City", cities, key="rt_city")
    weather_data = redis_conn.get_current_weather(selected_city)
    
    if weather_data:
        create_metric_cards(weather_data)
        st.divider()
        st.subheader("📊 Current Conditions")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.plotly_chart(create_gauge_chart(weather_data.get('temperature', 0), "Temp (°C)", 50, 'red'), use_container_width=True)
        with c2:
            st.plotly_chart(create_gauge_chart(weather_data.get('humidity', 0), "Humidity (%)", 100, 'blue'), use_container_width=True)
        with c3:
            st.plotly_chart(create_gauge_chart(weather_data.get('wind_speed', 0), "Wind (km/h)", 100, 'green'), use_container_width=True)
    
    redis_conn.close() # Quan trọng để không treo kết nối