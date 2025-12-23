"""
Forecast Tab
Hiển thị dự báo 7 ngày từ PostgreSQL
(Đã đồng bộ với PostgresConnector: get_latest_predictions)
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.postgres_connector import PostgresConnector
# Giả sử file config nằm ở thư mục cha
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
    
    # Vẽ đường dữ liệu thực tế (nếu có và không full Null)
    if actual_col in df.columns and df[actual_col].notna().any():
        fig.add_trace(go.Scatter(
            x=df['datetime'], 
            y=df[actual_col], 
            name='Actual', 
            mode='lines', 
            line=dict(color='blue', width=2)
        ))
    
    # Vẽ đường dữ liệu dự báo
    if predicted_col in df.columns and df[predicted_col].notna().any():
        fig.add_trace(go.Scatter(
            x=df['datetime'], 
            y=df[predicted_col], 
            name='Predicted', 
            mode='lines', 
            line=dict(color='red', dash='dash', width=2)
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
    st.subheader("📈 Model Accuracy (MAE)")
    
    # Danh sách cặp cột cần so sánh khớp với PostgresConnector
    # (Cột thực tế, Cột dự báo, Tên hiển thị, Đơn vị)
    features = [
        ('temp_celsius', 'prediction_temp_celsius', 'Temp', '°C'),
        ('humidity_pct', 'prediction_humidity_pct', 'Humidity', '%'),
        ('wind_speed_kmh', 'prediction_wind_speed_kmh', 'Wind', 'km/h')
    ]
    
    cols = st.columns(len(features))
    
    for idx, (act, pred, name, unit) in enumerate(features):
        if act in df.columns and pred in df.columns:
            # Chỉ tính toán trên các dòng có đủ cả 2 giá trị
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
    
    # 4. Lấy dữ liệu dự báo
    # SỬA LỖI: Gọi đúng hàm 'get_latest_predictions' thay vì 'get_forecast'
    df = pg_conn.get_latest_predictions(selected_city)
    
    pg_conn.close() # Đóng kết nối sớm cho nhẹ
    
    if df is not None and not df.empty:
        # Hiển thị chỉ số độ chính xác
        show_accuracy_metrics(df)
        
        st.divider()
        st.subheader("📉 Forecast Trends")
        
        # Vẽ biểu đồ Nhiệt độ (Full width)
        st.plotly_chart(
            create_forecast_chart(df, "Temperature", "prediction_temp_celsius", "temp_celsius", "°C"), 
            use_container_width=True
        )
        
        # Vẽ biểu đồ Độ ẩm & Gió (2 cột)
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(
                create_forecast_chart(df, "Humidity", "prediction_humidity_pct", "humidity_pct", "%"), 
                use_container_width=True
            )
        with c2:
            st.plotly_chart(
                create_forecast_chart(df, "Wind Speed", "prediction_wind_speed_kmh", "wind_speed_kmh", "km/h"), 
                use_container_width=True
            )
            
        st.divider()
        st.subheader("📋 Detailed Data View")
        
        # Format lại datetime cho đẹp
        df_display = df.copy()
        if 'datetime' in df_display.columns:
            df_display['datetime'] = pd.to_datetime(df_display['datetime']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Chỉ hiển thị các cột quan trọng
        # Tên cột phải khớp với SQL Alias trong PostgresConnector
        display_cols = [
            'datetime', 
            'prediction_weather_condition', 
            'prediction_temp_celsius', 
            'prediction_humidity_pct', 
            'prediction_wind_speed_kmh'
        ]
        
        # Đổi tên cột hiển thị cho đẹp
        rename_map = {
            'datetime': 'Time',
            'prediction_weather_condition': 'Condition',
            'prediction_temp_celsius': 'Temp (°C)',
            'prediction_humidity_pct': 'Humidity (%)',
            'prediction_wind_speed_kmh': 'Wind (km/h)'
        }
        
        # Lọc các cột tồn tại và hiển thị
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