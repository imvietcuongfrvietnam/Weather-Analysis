from producers.base_producer import CsvKafkaProducer
from config.kafka_config import TOPIC_COLLISIONS


def main() -> None:
    producer = CsvKafkaProducer(
        topic=TOPIC_COLLISIONS,
        csv_path="nyc_collisions_2016.csv",
    )
    producer.send_all()


if __name__ == "__main__":
    main()


