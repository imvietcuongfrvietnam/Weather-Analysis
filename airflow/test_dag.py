from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
}


with DAG(
    'test_system_connectivity',
    default_args=default_args,
    description='Kiểm tra kết nối từ Airflow Pod tới các dịch vụ K8s',
    schedule_interval=None, # Chỉ chạy thủ công (manual trigger)
    start_date=days_ago(1),
    tags=['test', 'debug'],
) as dag:

    # Script kiểm tra tích hợp
    test_script = """
import psycopg2
import redis
import requests
import os
import socket

def check_port(host, port):
    try:
        socket.create_connection((host, port), timeout=3)
        return True
    except:
        return False

print("🔍 BẮT ĐẦU KIỂM TRA KẾT NỐI HỆ THỐNG...")

# 1. Kiểm tra DNS & Port căn bản
services = {
    "PostgreSQL": ("weather-postgresql.default.svc.cluster.local", 5432),
    "Redis": ("weather-redis.default.svc.cluster.local", 6379),
    "Kafka": ("weather-kafka.default.svc.cluster.local", 9092),
    "MinIO": ("weather-minio.default.svc.cluster.local", 9000)
}

for name, (host, port) in services.items():
    status = "✅ OPEN" if check_port(host, port) else "❌ CLOSED"
    print(f"Network {name} ({host}:{port}): {status}")

# 2. Kiểm tra xác thực PostgreSQL
try:
    conn = psycopg2.connect(
        host='weather-postgresql.default.svc.cluster.local',
        database='weather_db',
        user='weather_user',
        password='weather_pass',
        connect_timeout=3
    )
    print("✅ Postgres Auth: Thành công!")
    conn.close()
except Exception as e:
    print(f"❌ Postgres Auth: Thất bại! Lỗi: {e}")

# 3. Kiểm tra xác thực Redis
try:
    r = redis.Redis(host='weather-redis.default.svc.cluster.local', port=6379, socket_timeout=3)
    if r.ping():
        print("✅ Redis Auth: Thành công!")
except Exception as e:
    print(f"❌ Redis Auth: Thất bại! Lỗi: {e}")

# 4. Kiểm tra MinIO (HTTP)
try:
    resp = requests.get("http://weather-minio.default.svc.cluster.local:9000/minio/health/live", timeout=3)
    print(f"✅ MinIO Health: {resp.status_code}")
except Exception as e:
    print(f"❌ MinIO Health: Thất bại! Lỗi: {e}")

print("🏁 KẾT THÚC KIỂM TRA.")
"""

    run_test = KubernetesPodOperator(
        task_id='check_all_connections',
        name='connectivity-test-pod',
        namespace='airflow',
        image='weather-etl-app:v3',
        image_pull_policy='Never',
        cmds=["python3", "-c", test_script],
        is_delete_operator_pod=False,
        get_logs=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "256Mi", "cpu": "200m"},
            limits={"memory": "512Mi", "cpu": "500m"}        )
    )

    run_test