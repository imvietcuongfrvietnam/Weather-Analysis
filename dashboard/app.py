"""
Main Dashboard Application
Điểm vào chính của ứng dụng Streamlit
Updated: Safe Config Loading
"""

import streamlit as st
import time
import os
import sys

# Setup Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import config
except ImportError:
    class Config:
        DASHBOARD_TITLE = "Weather Forecast Dashboard"
        REDIS_HOST = "weather-redis"
        POSTGRES_HOST = "weather-postgresql"
        DASHBOARD_REFRESH_SECONDS = 10
    config = Config()

# Fallback nếu config thiếu biến
if not hasattr(config, 'DASHBOARD_TITLE'): config.DASHBOARD_TITLE = "Weather Forecast Dashboard"
if not hasattr(config, 'REDIS_HOST'): config.REDIS_HOST = "weather-redis"
if not hasattr(config, 'POSTGRES_HOST'): config.POSTGRES_HOST = "weather-postgresql"
if not hasattr(config, 'DASHBOARD_REFRESH_SECONDS'): config.DASHBOARD_REFRESH_SECONDS = 10

from components.realtime_tab import show_realtime_tab
from components.forecast_tab import show_forecast_tab

# Page configuration
st.set_page_config(
    page_title=config.DASHBOARD_TITLE,
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        text-align: center;
        background: -webkit-linear-gradient(45deg, #3b82f6, #8b5cf6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 10px;
        border-left: 5px solid #3b82f6;
    }
    </style>
""", unsafe_allow_html=True)

# Main Header
st.markdown(f'<div class="main-header">{config.DASHBOARD_TITLE}</div>', unsafe_allow_html=True)

# --- SIDEBAR SETUP ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/4052/4052984.png", width=80)
    st.title("⚙️ Control Panel")
    st.divider()

    st.subheader("📡 Connection Status")
    st.success(f"Redis: {config.REDIS_HOST}")
    st.info(f"Postgres: {config.POSTGRES_HOST}")
    
    st.divider()
    
    st.subheader("🔄 Update Mode")
    auto_refresh = st.toggle("Enable Live Stream", value=True)
    
    if auto_refresh:
        refresh_rate = st.slider("Refresh Rate (s)", 5, 60, config.DASHBOARD_REFRESH_SECONDS)
    else:
        if st.button("Refresh Now", type="primary"):
            st.rerun()

    st.divider()
    st.caption("v1.0.0 - Spark Lambda Architecture")

# --- MAIN CONTENT ---
tab1, tab2 = st.tabs(["🔥 Real-Time Monitoring", "📊 7-Day ML Forecast"])

with tab1:
    try:
        show_realtime_tab()
    except Exception as e:
        st.error(f"Error loading Real-time Tab: {e}")

with tab2:
    try:
        show_forecast_tab()
    except Exception as e:
        st.error(f"Error loading Forecast Tab: {e}")

# --- LOGIC AUTO-REFRESH ---
IS_TEST_MODE = os.getenv("STREAMLIT_TEST_MODE", "false").lower() == "true"

if auto_refresh and not IS_TEST_MODE:
    time.sleep(refresh_rate)
    st.rerun()
elif IS_TEST_MODE:
    st.info("🛠️ Test Mode Active: Auto-refresh disabled")