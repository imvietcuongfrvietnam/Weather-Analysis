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
    # Lưu ý: Chạy mỗi phút là rất nhanh, đảm bảo max_active_runs=1 để không bị chồng chéo
    schedule_interval='*/1 * * * *', 
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'ml', 'lambda'],
) as dag:

    ml_task = KubernetesPodOperator(
        task_id='run_ml_forecast',
        name='weather-ml-job',
        namespace='airflow',
        image='weather-etl-app:v5', # Đảm bảo bạn đã build image này
        image_pull_policy='Never',
        
        # Chạy quyền Root để tránh lỗi permission khi tải Jar
        security_context={
            "runAsUser": 0,
        },
        
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            
            # --- SỬA QUAN TRỌNG: Thêm thư viện Hadoop AWS để đọc MinIO ---
            # Kết hợp cả Postgres (ghi DB) và Hadoop-AWS (đọc MinIO)
            "--packages", "org.postgresql:postgresql:42.5.0,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
            # -------------------------------------------------------------
            
            "--driver-memory", "1g", # Tăng lên 1G cho an toàn với Job ML
            "--executor-memory", "1g",
            
            # Đảm bảo tên file này ĐÚNG với file trong image của bạn 
            # (Kiểm tra xem là spark_ml_job.py hay main_ml.py?)
            "/app/job/spark_ml_job.py" 
        ],
        
        env_vars={
            "PYSPARK_PYTHON": "/usr/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
            
            # Cập nhật PYTHONPATH đầy đủ để import các module con
            "PYTHONPATH": "/app:/app/job:/app/readers:/app/writers",
            
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
        },
        
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "400m"},
            limits={"memory": "2Gi", "cpu": "1000m"} # Tăng limit lên một chút để tránh OOM
        ),
        
        get_logs=True,
        is_delete_operator_pod=False,
    )