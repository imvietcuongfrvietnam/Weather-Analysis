"""
Forecast Tab
Hiển thị dự báo 7 ngày từ PostgreSQL
(Đã cập nhật: Hỗ trợ Wind Direction và Future Forecast)
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.postgres_connector import PostgresConnector
try:
    import config
except ImportError:
    pass

def create_forecast_chart(df, feature_name, predicted_col, actual_col, unit=""):
    """Tạo biểu đồ so sánh Thực tế vs Dự báo (Actual vs Predicted)"""
    fig = go.Figure()
    
        # Sắp xếp theo thời gian
    if 'datetime' in df.columns:
        df = df.sort_values('datetime')
    
    # Vẽ đường dữ liệu thực tế (Actual) 
    # Plotly sẽ tự động ngắt nét khi gặp giá trị NULL (trong trường hợp dữ liệu tương lai)
    if actual_col in df.columns:
        # Lọc bỏ bớt điểm null để check xem có dữ liệu không
        if df[actual_col].notna().any():
            fig.add_trace(go.Scatter(
                x=df['datetime'], 
                y=df[actual_col], 
                name='Actual', 
                mode='lines', 
                line=dict(color='dodgerblue', width=2)
            ))
    
    # Vẽ đường dữ liệu dự báo (Predicted) - Màu Đỏ nét đứt
    if predicted_col in df.columns:
        if df[predicted_col].notna().any():
            fig.add_trace(go.Scatter(
                x=df['datetime'], 
                y=df[predicted_col], 
                name='Predicted', 
                mode='lines', 
                line=dict(color='tomato', dash='dash', width=2)
            ))
    
    fig.update_layout(
        title=f"{feature_name} Forecast ({unit})", 
        height=350, 
        hovermode='x unified',
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def show_accuracy_metrics(df):
    """Hiển thị các chỉ số MAE (Mean Absolute Error) cho mô hình"""
    st.subheader("📈 Model Accuracy (MAE on Historical Data)")
    
    # Danh sách cặp cột cần so sánh (Cột thực tế, Cột dự báo, Tên hiển thị, Đơn vị)
    # Lưu ý: Cần đảm bảo PostgresConnector đã SELECT các cột này (AS ...)
    features = [
        ('temp_celsius', 'prediction_temp_celsius', 'Temp', '°C'),
        ('humidity_pct', 'prediction_humidity_pct', 'Humidity', '%'),
        ('wind_speed_kmh', 'prediction_wind_speed_kmh', 'Wind Spd', 'km/h'),
        ('wind_direction', 'prediction_wind_direction', 'Wind Dir', '°') # Mới thêm
    ]
    
    cols = st.columns(len(features))
    
    for idx, (act, pred, name, unit) in enumerate(features):
        if act in df.columns and pred in df.columns:
            # Chỉ tính toán trên các dòng có đủ cả 2 giá trị (Tức là quá khứ)
            mask = df[act].notna() & df[pred].notna()
            
            if mask.any():
                mae = (df.loc[mask, act] - df.loc[mask, pred]).abs().mean()
                cols[idx].metric(label=f"{name} MAE", value=f"{mae:.2f} {unit}")
            else:
                cols[idx].metric(label=f"{name} MAE", value="N/A")

def show_forecast_tab():
    """Hàm chính hiển thị tab Dự báo"""
    st.header("📊 7-Day Weather Forecast")
    
    # 1. Kết nối Database
    pg_conn = PostgresConnector()
    if not pg_conn.connect():
        st.error("❌ Could not connect to PostgreSQL. Check connection settings.")
        return
    
    # 2. Lấy danh sách thành phố
    cities = pg_conn.get_available_cities()
    if not cities:
        st.warning("⚠️ No forecast data found in PostgreSQL yet.")
        pg_conn.close()
        return
    
    # 3. Dropdown chọn thành phố
    selected_city = st.selectbox("Select City", cities, key="fore_city_select")
    
    # 4. Lấy dữ liệu dự báo (Limit mặc định trong connector nên tăng lên để lấy đủ cả quá khứ + tương lai)
    # Ví dụ: 7 ngày quá khứ + 7 ngày tương lai = 14 ngày * 24h = 336 dòng
    df = pg_conn.get_latest_predictions(selected_city, limit=336)
    
    pg_conn.close()
    
    if df is not None and not df.empty:
        # Hiển thị chỉ số độ chính xác (Chỉ tính trên phần dữ liệu có Actual)
        show_accuracy_metrics(df)
        
        st.divider()
        st.subheader("📉 Forecast Trends")
        
        # Row 1: Nhiệt độ & Độ ẩm
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(
                create_forecast_chart(df, "Temperature", "prediction_temp_celsius", "temp_celsius", "°C"), 
                use_container_width=True
            )
        with c2:
            st.plotly_chart(
                create_forecast_chart(df, "Humidity", "prediction_humidity_pct", "humidity_pct", "%"), 
                use_container_width=True
            )
        
        # Row 2: Gió (Tốc độ & Hướng)
        c3, c4 = st.columns(2)
        with c3:
            st.plotly_chart(
                create_forecast_chart(df, "Wind Speed", "prediction_wind_speed_kmh", "wind_speed_kmh", "km/h"), 
                use_container_width=True
            )
        with c4:
             st.plotly_chart(
                create_forecast_chart(df, "Wind Direction", "prediction_wind_direction", "wind_direction", "°"), 
                use_container_width=True
            )
            
        st.divider()
        st.subheader("📋 Detailed Data View (Next 7 Days)")
        
        # Format lại datetime
        df_display = df.copy()
        if 'datetime' in df_display.columns:
            df_display['datetime'] = pd.to_datetime(df_display['datetime']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Các cột hiển thị
        rename_map = {
            'datetime': 'Time',
            'prediction_weather_condition': 'Condition',
            'prediction_temp_celsius': 'Temp (°C)',
            'prediction_humidity_pct': 'Humidity (%)',
            'prediction_wind_speed_kmh': 'Wind Spd (km/h)',
            'prediction_wind_direction': 'Wind Dir (°)'
        }
        
        # Lọc lấy các cột dự báo để hiển thị bảng
        display_cols = list(rename_map.keys())
        available_cols = [c for c in display_cols if c in df_display.columns]
        
        st.dataframe(
            df_display[available_cols].rename(columns=rename_map), 
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning(f"⚠️ No data available for {selected_city}")

if __name__ == "__main__":
    st.set_page_config(layout="wide")
    show_forecast_tab()