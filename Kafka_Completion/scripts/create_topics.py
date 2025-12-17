import sys
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH để có thể import config và producers
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

from kafka.admin import KafkaAdminClient, NewTopic

from config.kafka_config import (
    BOOTSTRAP_SERVERS,
    TOPIC_311,
    TOPIC_COLLISIONS,
    TOPIC_EMERGENCY_ALERTS,
)


def create_topic_if_not_exists(admin: KafkaAdminClient, topic_name: str) -> None:
    existing_topics = admin.list_topics()
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' đã tồn tại, bỏ qua.")
        return

    topic = NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=1,
    )
    admin.create_topics([topic])
    print(f"Đã tạo topic '{topic_name}'.")


def main() -> None:
    # Thêm api_version để tránh lỗi UnrecognizedBrokerVersion
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=(0, 10, 1)  # Chỉ định API version tương thích
    )

    for topic in [TOPIC_311, TOPIC_COLLISIONS, TOPIC_EMERGENCY_ALERTS]:
        create_topic_if_not_exists(admin, topic)

    admin.close()


if __name__ == "__main__":
    main()


