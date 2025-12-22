"""
Visualization - Plot Weather Forecasts
Trực quan hóa kết quả dự đoán thời tiết
Updated: Fix metric keys (R2 -> r2) and Config Import
"""

import matplotlib
matplotlib.use('Agg')  # Backend không GUI (quan trọng cho Server/Docker)
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import sys
import os

# --- IMPORT CONFIG ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import config
except ImportError:
    # Fallback Config
    class Config:
        LOCAL_PLOTS_DIR = "./plots_output"
        CONTINUOUS_FEATURES = ["temperature", 
        "humidity", 
        "pressure", 
        "wind_speed", 
        "wind_direction"]
        CATEGORICAL_FEATURES = ["weather_desc"]
    config = Config()

class ForecastVisualizer:
    """Create visualizations for weather forecasts"""
    
    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or config.LOCAL_PLOTS_DIR
        # Tạo thư mục nếu chưa tồn tại
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # Set style (tùy chỉnh lại để không phụ thuộc seaborn nếu chưa cài)
        try:
            plt.style.use('seaborn-v0_8-darkgrid')
        except:
            plt.style.use('ggplot')
    
    def plot_single_feature(self, df_pandas: pd.DataFrame, target_feature: str,
                           datetime_col: str = 'datetime',
                           save: bool = True):
        """
        Vẽ biểu đồ so sánh Thực tế vs Dự đoán cho 1 đặc trưng
        """
        prediction_col = f"prediction_{target_feature}"
        
        if prediction_col not in df_pandas.columns or target_feature not in df_pandas.columns:
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        
        # Sắp xếp theo thời gian để vẽ line chart không bị rối
        df_sorted = df_pandas.sort_values(by=datetime_col)
        
        # Plot Actual
        ax.plot(df_sorted[datetime_col], df_sorted[target_feature],
                label='Actual', color='dodgerblue', linewidth=2, alpha=0.8)
        
        # Plot Predicted
        ax.plot(df_sorted[datetime_col], df_sorted[prediction_col],
                label='Predicted', color='tomato', linewidth=2, alpha=0.8, linestyle='--')
        
        # Formatting
        ax.set_ylabel(target_feature.replace('_', ' ').title(), fontsize=12)
        ax.set_title(f'Forecast: {target_feature}', fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        if save:
            filename = os.path.join(self.output_dir, f"{target_feature}_forecast.png")
            plt.savefig(filename, dpi=100)
            print(f"   📊 Saved plot: {filename}")
        
        plt.close()
    
    def plot_all_features(self, df_pandas: pd.DataFrame, datetime_col: str = 'datetime'):
        """
        Vẽ tất cả các biểu đồ
        """
        print(f"\n📊 Creating forecast visualizations...")
        
        # 1. Continuous Features
        for target in config.CONTINUOUS_FEATURES:
            self.plot_single_feature(df_pandas, target, datetime_col)
        
        # 2. Categorical Features
        if hasattr(config, 'CATEGORICAL_FEATURES'):
            for target in config.CATEGORICAL_FEATURES:
                self.plot_categorical_feature(df_pandas, target, datetime_col)
        
        print(f"   ✅ All plots saved to {self.output_dir}")
    
    def plot_categorical_feature(self, df_pandas: pd.DataFrame, target_feature: str,
                                 datetime_col: str = 'datetime'):
        """
        Vẽ biểu đồ Scatter cho dữ liệu phân loại (VD: Mưa, Nắng)
        """
        prediction_col = f"prediction_{target_feature}"
        
        if prediction_col not in df_pandas.columns:
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        df_sorted = df_pandas.sort_values(by=datetime_col)
        
        # Convert categories to numeric for plotting
        # Lấy tập hợp tất cả các giá trị (cả thực tế và dự đoán)
        all_cats = pd.concat([df_sorted[target_feature], df_sorted[prediction_col]]).dropna().unique()
        categories = sorted(all_cats)
        category_map = {cat: idx for idx, cat in enumerate(categories)}
        
        actual_numeric = df_sorted[target_feature].map(category_map)
        predicted_numeric = df_sorted[prediction_col].map(category_map)
        
        # Plot Scatter
        ax.scatter(df_sorted[datetime_col], actual_numeric,
                  label='Actual', color='dodgerblue', alpha=0.6, s=60, marker='o')
        ax.scatter(df_sorted[datetime_col], predicted_numeric,
                  label='Predicted', color='tomato', alpha=0.6, s=60, marker='x')
        
        # Set y-ticks
        ax.set_yticks(range(len(categories)))
        ax.set_yticklabels(categories)
        
        ax.set_title(f'Forecast: {target_feature}', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Format Date
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        filename = os.path.join(self.output_dir, f"{target_feature}_forecast.png")
        plt.savefig(filename, dpi=100)
        print(f"   📊 Saved plot: {filename}")
        plt.close()
    
    def plot_metrics_comparison(self, all_metrics: dict):
        """
        So sánh hiệu năng các mô hình (Dựa trên R2 Score)
        """
        print(f"\n📊 Creating metrics comparison plot...")
        
        features = []
        r2_scores = []
        
        for target in config.CONTINUOUS_FEATURES:
            # QUAN TRỌNG: Sửa 'R2' thành 'r2' (khớp với forecast_evaluator.py)
            if target in all_metrics and 'r2' in all_metrics[target]:
                features.append(target)
                r2_scores.append(all_metrics[target]['r2'])
        
        if not features:
            print("   ⚠️  No metrics found to plot")
            return
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(features, r2_scores, color='steelblue', alpha=0.7)
        
        # Tô màu dựa trên độ chính xác
        for bar, score in zip(bars, r2_scores):
            if score >= 0.8: bar.set_color('forestgreen')  # Tốt
            elif score >= 0.5: bar.set_color('orange')     # Khá
            else: bar.set_color('firebrick')               # Kém
        
        ax.set_ylabel('R² Score (Higher is Better)')
        ax.set_title('Model Accuracy Comparison')
        ax.set_ylim([0, 1.1])
        
        # Hiển thị số trên cột
        for bar, score in zip(bars, r2_scores):
            height = bar.get_height() if bar.get_height() > 0 else 0
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                   f'{score:.2f}', ha='center', fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        filename = os.path.join(self.output_dir, "model_accuracy.png")
        plt.savefig(filename, dpi=100)
        print(f"   📊 Saved metrics plot: {filename}")
        plt.close()
    
    def plot_multi_panel_dashboard(self, df_pandas: pd.DataFrame, datetime_col='datetime'):
        """Vẽ Dashboard tổng hợp (Optional)"""
        # Logic tương tự plot_all_features nhưng gộp vào 1 ảnh
        pass 

if __name__ == "__main__":
    print("Visualization Module Loaded.")