from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_spark_ml_forecast',
    default_args=default_args,
    description='Chạy Spark ML Training & Forecast (Batch Layer)',
    schedule_interval='*/1 * * * *', 
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'ml', 'lambda'],
) as dag:

    ml_task = KubernetesPodOperator(
        task_id='run_ml_forecast',
        name='weather-ml-job',
        namespace='airflow',
        image='weather-etl-app:v3',
        image_pull_policy='Never',
        
        # SỬA QUAN TRỌNG: Chạy quyền Root để tải driver Postgres thành công
        security_context={
            "runAsUser": 0,
        },
        
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            # SỬA QUAN TRỌNG: Chuyển Ivy cache sang /tmp
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--packages", "org.postgresql:postgresql:42.5.0",
            "--driver-memory", "800m",
            "/app/job/spark_ml_job.py"
        ],
        
        env_vars={
            "PYSPARK_PYTHON": "/usr/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
            "MINIO_ENDPOINT": "http://weather-minio.default.svc.cluster.local:9000",
            "MINIO_ACCESS_KEY": "admin",
            "MINIO_SECRET_KEY": "password123",
            "MINIO_BUCKET": "weather-data",
            "POSTGRES_HOST": "weather-postgresql.default.svc.cluster.local",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "weather_db",
            "POSTGRES_USER": "weather_user",
            "POSTGRES_PASSWORD": "weather_pass",
            "POSTGRES_TABLE": "weather_predictions",
            "PYTHONPATH": "/app"
        },
        
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "400m"},
            limits={"memory": "1.5Gi", "cpu": "1000m"}
        ),
        
        get_logs=True,
        is_delete_operator_pod=True,
    )