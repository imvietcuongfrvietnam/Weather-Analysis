"""
Weather Forecast Dashboard
Main Streamlit Application

Real-time weather from Redis + 7-day forecast from PostgreSQL
"""

import streamlit as st
import time
from components.realtime_tab import show_realtime_tab
from components.forecast_tab import show_forecast_tab
import config

# Page configuration
st.set_page_config(
    page_title=config.DASHBOARD_TITLE,
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #1e3a8a 0%, #3b82f6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .stMetric {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

# Main header
st.markdown(f'<h1 class="main-header">{config.DASHBOARD_TITLE}</h1>', unsafe_allow_html=True)
st.caption("Real-time weather monitoring and 7-day ML forecasts")

# Sidebar
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/1163/1163661.png", width=100)
    st.title("⚙️ Settings")
    
    st.divider()
    
    # Configuration info
    st.subheader("📡 Data Sources")
    st.text(f"Redis: {config.REDIS_HOST}:{config.REDIS_PORT}")
    st.text(f"PostgreSQL: {config.POSTGRES_HOST}:{config.POSTGRES_PORT}")
    
    st.divider()
    
    # Refresh rate
    st.subheader("🔄 Refresh Rate")
    st.text(f"Auto-refresh: {config.DASHBOARD_REFRESH_SECONDS}s")
    st.caption("Real-time tab updates automatically")
    
    st.divider()
    
    # About
    st.subheader("ℹ️ About")
    st.markdown("""
    **Weather Forecast System**
    
    - 🔥 Real-time data from Redis
    - 📊 ML predictions from PostgreSQL
    - ⚡ Spark ETL & ML pipelines
    - 📈 Interactive visualizations
    """)
    
    st.divider()
    
    # Manual refresh button
    if st.button("🔄 Force Refresh", use_container_width=True):
        st.rerun()

# Main content - Tabs
tab1, tab2 = st.tabs(["🔥 Real-Time Weather", "📊 7-Day Forecast"])

with tab1:
    # Real-time tab with auto-refresh
    show_realtime_tab()
    
    # Auto-refresh countdown
    st.divider()
    placeholder = st.empty()
    
    # Countdown timer
    for i in range(config.DASHBOARD_REFRESH_SECONDS, 0, -1):
        placeholder.caption(f"🔄 Auto-refresh in {i}s...")
        time.sleep(1)
    
    # Trigger re-run for auto-refresh
    placeholder.caption("🔄 Refreshing...")
    st.rerun()

with tab2:
    # Forecast tab (static)
    show_forecast_tab()

# Footer
st.divider()
col1, col2, col3 = st.columns(3)

with col1:
    st.caption("🌦️ Weather Forecast Dashboard v1.0")

with col2:
    st.caption("Powered by Spark, Redis, PostgreSQL")

with col3:
    st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
