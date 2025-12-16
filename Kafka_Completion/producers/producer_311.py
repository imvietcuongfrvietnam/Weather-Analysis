from producers.base_producer import CsvKafkaProducer
from config.kafka_config import TOPIC_311


def main() -> None:
    producer = CsvKafkaProducer(
        topic=TOPIC_311,
        csv_path="nyc_311_2016_full.csv",
    )
    producer.send_all()


if __name__ == "__main__":
    main()


