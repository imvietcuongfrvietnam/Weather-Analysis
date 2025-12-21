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

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_predictions (
        city VARCHAR(50),
        forecast_date DATE,
        temperature_celsius DOUBLE PRECISION,
        humidity_pct DOUBLE PRECISION,
        wind_speed_kmh DOUBLE PRECISION,
        weather_condition VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (city, forecast_date)
    );
    """

    init_db_task = KubernetesPodOperator(
        task_id='init_postgres_table',
        name='init-db-job',
        namespace='airflow',
        image='weather-etl-app:v3',
        image_pull_policy='Never',
        cmds=["python"],
        arguments=["-c", f"""
import psycopg2
import os

try:
    conn = psycopg2.connect(
        host='weather-postgresql.default.svc.cluster.local',
        database='weather_db',
        user='weather_user',
        password='weather_pass'
    )
    cur = conn.cursor()
    cur.execute("{create_table_sql.replace(chr(10), " ")}")
    conn.commit()
    print("✅ Table created successfully")
except Exception as e:
    print(f"❌ Error: {{e}}")
        """],
        is_delete_operator_pod=True,
    )

    trigger_streaming = TriggerDagRunOperator(
        task_id='trigger_streaming_dag',
        trigger_dag_id='weather_spark_streaming_etl',
    )
    
    trigger_ml = TriggerDagRunOperator(
        task_id='trigger_ml_dag',
        trigger_dag_id='weather_spark_ml_forecast',
    )

    init_db_task >> [trigger_streaming, trigger_ml]