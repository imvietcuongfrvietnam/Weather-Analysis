"""
Forecast Tab
Hiển thị dự báo 7 ngày từ PostgreSQL với tên cột đã ánh xạ chuẩn
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.postgres_connector import PostgresConnector
import config

def create_forecast_chart(df, feature_name, predicted_col, actual_col, unit=""):
    """Tạo biểu đồ so sánh Thực tế vs Dự báo (Actual vs Predicted)"""
    fig = go.Figure()
    
    # Vẽ đường dữ liệu thực tế (nếu có)
    if actual_col in df.columns:
        fig.add_trace(go.Scatter(x=df['datetime'], y=df[actual_col], name='Actual', mode='lines', line=dict(color='blue')))
    
    # Vẽ đường dữ liệu dự báo
    if predicted_col in df.columns:
        fig.add_trace(go.Scatter(x=df['datetime'], y=df[predicted_col], name='Predicted', mode='lines', line=dict(color='red', dash='dash')))
    
    fig.update_layout(title=f"{feature_name} Forecast", height=400, hovermode='x unified')
    return fig

def show_accuracy_metrics(df):
    """Hiển thị các chỉ số MAE (Mean Absolute Error) cho mô hình"""
    st.subheader("📈 Model Accuracy (MAE)")
    # Ánh xạ theo Alias trong PostgresConnector: temperature -> actual_temp, prediction_temperature -> predicted_temp
    features = [
        ('actual_temp', 'predicted_temp', 'Temp', '°C'),
        ('actual_humidity', 'predicted_humidity', 'Humidity', '%'),
        ('actual_wind', 'predicted_wind', 'Wind', 'km/h')
    ]
    
    cols = st.columns(len(features))
    for idx, (act, pred, name, unit) in enumerate(features):
        if act in df.columns and pred in df.columns:
            # Lọc bỏ giá trị Null trước khi tính toán để tránh treo
            mask = df[act].notna() & df[pred].notna()
            if mask.any():
                mae = (df.loc[mask, act] - df.loc[mask, pred]).abs().mean()
                cols[idx].metric(label=f"{name} MAE", value=f"{mae:.2f} {unit}")

def show_forecast_tab():
    """Hàm chính hiển thị tab Dự báo"""
    st.header("📊 7-Day Weather Forecast")
    
    pg_conn = PostgresConnector()
    if not pg_conn.connect():
        st.error("❌ Could not connect to PostgreSQL")
        return
    
    cities = pg_conn.get_available_cities()
    if not cities:
        st.warning("⚠️ No forecast data in PostgreSQL yet.")
        pg_conn.close()
        return
    
    selected_city = st.selectbox("Select City", cities, key="fore_city")
    
    # get_forecast trả về DataFrame đã alias các cột (actual_temp, predicted_temp, ...)
    df = pg_conn.get_forecast(selected_city)
    
    if df is not None and not df.empty:
        show_accuracy_metrics(df)
        st.divider()
        
        # Biểu đồ nhiệt độ
        st.plotly_chart(create_forecast_chart(df, "Temperature", "predicted_temp", "actual_temp", "°C"), use_container_width=True)
        
        # Biểu đồ Độ ẩm & Tốc độ gió
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(create_forecast_chart(df, "Humidity", "predicted_humidity", "actual_humidity", "%"), use_container_width=True)
        with c2:
            st.plotly_chart(create_forecast_chart(df, "Wind Speed", "predicted_wind", "actual_wind", "km/h"), use_container_width=True)
            
        st.subheader("📋 Detailed Forecast Data")
        # Hiển thị bảng dữ liệu tóm tắt
        display_cols = ['datetime', 'predicted_temp', 'predicted_humidity', 'predicted_desc']
        available_display = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_display], use_container_width=True)
    
    pg_conn.close()