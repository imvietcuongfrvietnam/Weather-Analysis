from producers.base_producer import CsvKafkaProducer
from config.kafka_config import TOPIC_EMERGENCY_ALERTS


def main() -> None:
    producer = CsvKafkaProducer(
        topic=TOPIC_EMERGENCY_ALERTS,
        csv_path="nyc_emergency_alerts_2012_2017_full.csv",
    )
    producer.send_all()


if __name__ == "__main__":
    main()


