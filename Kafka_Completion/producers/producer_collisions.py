import sys
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH để có thể import config và producers
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

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


