from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    'weather_system_init',
    default_args=default_args,
    description='Khởi tạo bảng Database và kích hoạt Streaming',
    schedule_interval='@once',
    start_date=days_ago(1),
    tags=['setup', 'weather'],
) as dag:

    # --- SQL CẬP NHẬT THEO SCHEMA SPARK ML ---
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_predictions (
        datetime TIMESTAMP,
        city VARCHAR(50),
        temperature DOUBLE PRECISION,
        prediction_temperature DOUBLE PRECISION,
        humidity DOUBLE PRECISION,
        prediction_humidity DOUBLE PRECISION,
        pressure DOUBLE PRECISION,
        prediction_pressure DOUBLE PRECISION,
        wind_speed DOUBLE PRECISION,
        prediction_wind_speed DOUBLE PRECISION,
        precipitation_mm DOUBLE PRECISION,
        prediction_precipitation_mm DOUBLE PRECISION,
        weather_desc VARCHAR(100),
        prediction_weather_desc VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (city, datetime)
    );
    """

    init_db_task = KubernetesPodOperator(
        task_id='init_postgres_table',
        name='init-db-job',
        namespace='airflow',
        image='weather-etl-app:v3', # Đảm bảo image này có cài psycopg2
        image_pull_policy='Never',
        cmds=["python3"],
        arguments=["-c", f"""
import psycopg2
import os

try:
    # Cập nhật theo file deploy/postgre.yaml
    conn = psycopg2.connect(
        host='weather-postgresql.default.svc.cluster.local',
        port=5432,
        database='weather_db',
        user='weather_user',
        password='weather_pass'
    )
    cur = conn.cursor()
    # Thực thi SQL tạo bảng
    cur.execute(\"\"\"{create_table_sql}\"\"\")
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Table 'weather_predictions' initialized successfully")
except Exception as e:
    print(f"❌ Database Init Error: {{e}}")
    exit(1) # Fail task nếu không tạo được bảng
        """],
        is_delete_operator_pod=True,
    )

    # Kích hoạt Spark Streaming (Lambda Speed Layer)
    trigger_streaming = TriggerDagRunOperator(
        task_id='trigger_streaming_dag',
        trigger_dag_id='weather_spark_streaming_etl',
    )
    
    # Kích hoạt ML Training/Forecast (Lambda Batch Layer)
    trigger_ml = TriggerDagRunOperator(
        task_id='trigger_ml_dag',
        trigger_dag_id='weather_spark_ml_forecast',
    )

    init_db_task >> [trigger_streaming, trigger_ml]