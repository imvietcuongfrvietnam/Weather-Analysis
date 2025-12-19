"""
Visualization - Plot Weather Forecasts
Trực quan hóa kết quả dự đoán thời tiết
"""

import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend for server environments
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import pandas as pd
import os
import config

class ForecastVisualizer:
    """Create visualizations for weather forecasts"""
    
    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or config.LOCAL_PLOTS_DIR
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
    
    def plot_single_feature(self, df_pandas: pd.DataFrame, target_feature: str,
                           datetime_col: str = 'datetime',
                           save: bool = True):
        """
        Plot actual vs predicted for a single feature
        
        Args:
            df_pandas: Pandas DataFrame with datetime, actual, and predicted values
            target_feature: Name of target feature
            datetime_col: Name of datetime column
            save: Whether to save the plot
        """
        prediction_col = f"prediction_{target_feature}"
        
        if prediction_col not in df_pandas.columns:
            print(f"   ⚠️  No predictions for {target_feature}, skipping plot")
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        
        # Plot actual values
        ax.plot(df_pandas[datetime_col], df_pandas[target_feature],
                label='Actual', color='blue', linewidth=2, alpha=0.7)
        
        # Plot predictions
        ax.plot(df_pandas[datetime_col], df_pandas[prediction_col],
                label='Predicted', color='red', linewidth=2, alpha=0.7, linestyle='--')
        
        # Formatting
        ax.set_xlabel('DateTime', fontsize=12)
        ax.set_ylabel(target_feature.replace('_', ' ').title(), fontsize=12)
        ax.set_title(f'Weather Forecast: {target_feature.replace("_", " ").title()}',
                    fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        
        if save:
            filename = os.path.join(self.output_dir, f"{target_feature}_forecast.png")
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            print(f"   💾 Saved plot: {filename}")
        
        plt.close()
    
    def plot_all_features(self, df_pandas: pd.DataFrame, datetime_col: str = 'datetime'):
        """
        Create plots for all target features
        
        Args:
            df_pandas: Pandas DataFrame with all predictions
            datetime_col: Name of datetime column
        """
        print(f"\n📊 Creating forecast visualizations...")
        
        # Plot each continuous feature
        for target in config.CONTINUOUS_FEATURES:
            if target in df_pandas.columns:
                self.plot_single_feature(df_pandas, target, datetime_col)
        
        # Plot categorical feature (if applicable)
        for target in config.CATEGORICAL_FEATURES:
            if target in df_pandas.columns:
                # For categorical, we can create a different style plot
                self.plot_categorical_feature(df_pandas, target, datetime_col)
        
        print(f"   ✅ All plots saved to {self.output_dir}")
    
    def plot_categorical_feature(self, df_pandas: pd.DataFrame, target_feature: str,
                                 datetime_col: str = 'datetime'):
        """
        Plot categorical feature predictions
        
        Args:
            df_pandas: Pandas DataFrame
            target_feature: Name of categorical feature
            datetime_col: Name of datetime column
        """
        prediction_col = f"prediction_{target_feature}"
        
        if prediction_col not in df_pandas.columns:
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        
        # Convert categories to numeric for plotting
        categories = sorted(df_pandas[target_feature].dropna().unique())
        category_map = {cat: idx for idx, cat in enumerate(categories)}
        
        actual_numeric = df_pandas[target_feature].map(category_map)
        predicted_numeric = df_pandas[prediction_col].map(category_map)
        
        # Plot
        ax.scatter(df_pandas[datetime_col], actual_numeric,
                  label='Actual', color='blue', alpha=0.6, s=50)
        ax.scatter(df_pandas[datetime_col], predicted_numeric,
                  label='Predicted', color='red', alpha=0.6, s=50, marker='x')
        
        # Set y-ticks to category names
        ax.set_yticks(range(len(categories)))
        ax.set_yticklabels(categories)
        
        ax.set_xlabel('DateTime', fontsize=12)
        ax.set_ylabel('Weather Condition', fontsize=12)
        ax.set_title(f'Weather Condition Forecast', fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        
        filename = os.path.join(self.output_dir, f"{target_feature}_forecast.png")
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"   💾 Saved plot: {filename}")
        
        plt.close()
    
    def plot_multi_panel_dashboard(self, df_pandas: pd.DataFrame,
                                   datetime_col: str = 'datetime'):
        """
        Create a multi-panel dashboard with all features
        
        Args:
            df_pandas: Pandas DataFrame with all predictions
            datetime_col: Name of datetime column
        """
        print(f"\n📊 Creating dashboard...")
        
        # Create figure with subplots
        n_features = len(config.CONTINUOUS_FEATURES)
        fig, axes = plt.subplots(n_features, 1, figsize=(16, 4 * n_features))
        
        if n_features == 1:
            axes = [axes]
        
        for idx, target in enumerate(config.CONTINUOUS_FEATURES):
            if target not in df_pandas.columns:
                continue
            
            prediction_col = f"prediction_{target}"
            if prediction_col not in df_pandas.columns:
                continue
            
            ax = axes[idx]
            
            # Plot
            ax.plot(df_pandas[datetime_col], df_pandas[target],
                   label='Actual', color='blue', linewidth=2, alpha=0.7)
            ax.plot(df_pandas[datetime_col], df_pandas[prediction_col],
                   label='Predicted', color='red', linewidth=2, alpha=0.7, linestyle='--')
            
            # Formatting
            ax.set_ylabel(target.replace('_', ' ').title(), fontsize=11)
            ax.legend(loc='best', fontsize=9)
            ax.grid(True, alpha=0.3)
            
            # Only show x-label on bottom plot
            if idx == n_features - 1:
                ax.set_xlabel('DateTime', fontsize=12)
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            else:
                ax.set_xticklabels([])
        
        fig.suptitle('7-Day Weather Forecast Dashboard',
                    fontsize=16, fontweight='bold', y=0.995)
        
        plt.tight_layout()
        
        filename = os.path.join(self.output_dir, "forecast_dashboard.png")
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"   💾 Saved dashboard: {filename}")
        
        plt.close()
    
    def plot_metrics_comparison(self, all_metrics: dict):
        """
        Plot comparison of model performance metrics
        
        Args:
            all_metrics: Dictionary of metrics from ForecastEvaluator
        """
        print(f"\n📊 Creating metrics comparison plot...")
        
        # Extract R² scores for regression models
        features = []
        r2_scores = []
        
        for target in config.CONTINUOUS_FEATURES:
            if target in all_metrics and 'R2' in all_metrics[target]:
                features.append(target.replace('_', '\n'))
                r2_scores.append(all_metrics[target]['R2'])
        
        if not features:
            print("   ⚠️  No metrics to plot")
            return
        
        # Create bar plot
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(features, r2_scores, color='steelblue', alpha=0.7)
        
        # Color bars based on performance
        for i, (bar, score) in enumerate(zip(bars, r2_scores)):
            if score >= 0.8:
                bar.set_color('green')
            elif score >= 0.6:
                bar.set_color('orange')
            else:
                bar.set_color('red')
        
        ax.set_ylabel('R² Score', fontsize=12)
        ax.set_title('Model Performance Comparison (R² Score)', fontsize=14, fontweight='bold')
        ax.set_ylim([0, 1.0])
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bar, score in zip(bars, r2_scores):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{score:.3f}',
                   ha='center', va='bottom', fontsize=10)
        
        plt.tight_layout()
        
        filename = os.path.join(self.output_dir, "metrics_comparison.png")
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"   💾 Saved metrics plot: {filename}")
        
        plt.close()


if __name__ == "__main__":
    print("Forecast Visualization Module")
    print("Use ForecastVisualizer to create plots")
