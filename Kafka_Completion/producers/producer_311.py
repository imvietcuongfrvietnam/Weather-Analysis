import sys
from pathlib import Path

# Thêm thư mục gốc vào PYTHONPATH để có thể import config và producers
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

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


