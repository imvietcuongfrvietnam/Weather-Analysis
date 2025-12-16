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
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    for topic in [TOPIC_311, TOPIC_COLLISIONS, TOPIC_EMERGENCY_ALERTS]:
        create_topic_if_not_exists(admin, topic)

    admin.close()


if __name__ == "__main__":
    main()


