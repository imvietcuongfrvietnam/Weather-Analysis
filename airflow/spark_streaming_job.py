from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'weather_spark_streaming_etl',
    default_args=default_args,
    description='Chạy Spark Structured Streaming ETL (Speed Layer)',
    schedule_interval='@once', 
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'streaming', 'lambda'],
) as dag:

    streaming_task = KubernetesPodOperator(
        task_id='start_streaming_job',
        name='weather-streaming-job',
        namespace='airflow',
        image='weather-etl-app:v5',
        image_pull_policy='Never',
        
        # SỬA QUAN TRỌNG: Chạy quyền Root để có quyền ghi file hệ thống
        security_context={
            "runAsUser": 0,
        },
        
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            # CẬP NHẬT: Thêm hadoop-aws và aws-java-sdk-bundle vào đây
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
            "--driver-memory", "800m",
            "--executor-memory", "1g",
            "/app/job/main_etl.py"
        ],
        
        env_vars={
            "PYTHONPATH": "/app:/app/job:/app/readers:/app/writers", 
            "PYSPARK_PYTHON": "/usr/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
            "KAFKA_BOOTSTRAP_SERVERS": "weather-kafka.default.svc.cluster.local:9092",
            "MINIO_ENDPOINT": "http://weather-minio.default.svc.cluster.local:9000",
            "MINIO_ACCESS_KEY": "admin",
            "MINIO_SECRET_KEY": "password123",
            "MINIO_BUCKET": "weather-data",
            "REDIS_HOST": "weather-redis.default.svc.cluster.local",
            "REDIS_PORT": "6379",
            "REDIS_KEY_PREFIX": "weather:current"        },
        
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "400m"},
            limits={"memory": "1.5Gi", "cpu": "1000m"}
        ),
        
        get_logs=True,
        is_delete_operator_pod=False, 
    )