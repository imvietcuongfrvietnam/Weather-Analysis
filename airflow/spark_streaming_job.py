from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# --- THÊM IMPORT KUBERNETES MODELS ---
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_spark_streaming_etl',
    default_args=default_args,
    description='Chạy Spark Structured Streaming ETL',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'streaming'],
) as dag:

    streaming_task = KubernetesPodOperator(
        task_id='start_streaming_job',
        name='weather-streaming-job',
        namespace='airflow',
        image='weather-etl-app:v3',
        image_pull_policy='Never',
        
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            "--driver-memory", "1g",
            "--executor-memory", "1g",
            "/app/job/main_etl.py"
        ],
        
        env_vars={
            "KAFKA_BOOTSTRAP_SERVERS": "weather-kafka.default.svc.cluster.local:9092",
            "MINIO_ENDPOINT": "weather-minio.default.svc.cluster.local:9000",
            "MINIO_ACCESS_KEY": "admin",
            "MINIO_SECRET_KEY": "password123",
            "MINIO_BUCKET": "weather-data",
            "REDIS_HOST": "weather-redis.default.svc.cluster.local",
            "REDIS_PORT": "6379",
            "PYTHONPATH": "/app"
        },
        
        # --- SỬA LỖI RESOURCE Ở ĐÂY ---
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "500m"},
            limits={"memory": "1Gi", "cpu": "1000m"}
        ),
        
        get_logs=True,
        is_delete_operator_pod=False,
    )