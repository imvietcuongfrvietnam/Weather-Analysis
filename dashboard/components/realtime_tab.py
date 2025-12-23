"""
Real-Time Weather Tab
Hiển thị dữ liệu từ Redis với các tên cột chính xác:
datetime, City, temperature, humidity, pressure, weather_desc, wind_direction, wind_speed
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
    
    # 1. Lấy dữ liệu theo đúng danh sách cột bạn cung cấp
    temp = float(weather_data.get('temperature', 0))
    humidity = float(weather_data.get('humidity', 0))
    pressure = float(weather_data.get('pressure', 0))
    wind_spd = float(weather_data.get('wind_speed', 0))
    wind_dir = float(weather_data.get('wind_direction', 0))
    condition = weather_data.get('weather_desc', 'unknown')
    
    # Xử lý thời gian
    update_time = weather_data.get('datetime', datetime.now().isoformat())
    # Cắt chuỗi để chỉ lấy giờ (nếu định dạng chuẩn ISO) hoặc hiển thị nguyên
    time_display = update_time[-8:] if len(update_time) > 8 else update_time

    # 2. Hiển thị hàng 1: Các chỉ số cơ bản (4 cột)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="🌡️ Temperature", value=f"{temp:.1f}°C")
    with col2:
        st.metric(label="💧 Humidity", value=f"{humidity:.0f}%")
    with col3:
        st.metric(label="🌪️ Pressure", value=f"{pressure:.0f} hPa")
    with col4:
        st.metric(label="🌤️ Condition", value=condition.title())
    
    # 3. Hiển thị hàng 2: Gió và Thời gian (3 cột)
    col5, col6, col7 = st.columns(3)
    with col5:
        st.metric(label="💨 Wind Speed", value=f"{wind_spd:.1f} km/h")
    with col6:
        st.metric(label="🧭 Wind Direction", value=f"{wind_dir:.0f}°")
    with col7:
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
        st.warning("⚠️ No cities found in Redis. Please ensure the Streaming Job is running.")
        redis_conn.close()
        return
    
    # Dropdown chọn thành phố
    selected_city = st.selectbox("Select City", cities, key="rt_city")
    
    # Lấy dữ liệu
    weather_data = redis_conn.get_current_weather(selected_city)
    
    if weather_data:
        # Hiển thị thẻ số liệu
        create_metric_cards(weather_data)
        
        st.divider()
        st.subheader("📊 Live Gauges")
        
        # Hiển thị biểu đồ đồng hồ
        c1, c2, c3 = st.columns(3)
        with c1:
            val = float(weather_data.get('temperature', 0))
            st.plotly_chart(create_gauge_chart(val, "Temp (°C)", 50, 'red'), use_container_width=True)
        with c2:
            val = float(weather_data.get('humidity', 0))
            st.plotly_chart(create_gauge_chart(val, "Humidity (%)", 100, 'blue'), use_container_width=True)
        with c3:
            val = float(weather_data.get('wind_speed', 0))
            st.plotly_chart(create_gauge_chart(val, "Wind Speed (km/h)", 100, 'green'), use_container_width=True)
            
    else:
        st.info(f"Waiting for data update for {selected_city}...")
    
    redis_conn.close()