import csv
import json
import time
from pathlib import Path
from typing import Iterable, Dict, Any, Optional

from kafka import KafkaProducer

from config.kafka_config import (
    BOOTSTRAP_SERVERS,
    PRODUCER_FLUSH_INTERVAL,
    PRODUCER_SLEEP_SECONDS,
)


class CsvKafkaProducer:
    """
    Tiện ích chung để:
    - Đọc file CSV
    - Chuyển từng dòng thành dict
    - Gửi từng dòng lên Kafka dưới dạng JSON
    """

    def __init__(
        self,
        topic: str,
        csv_path: str,
        encoding: str = "utf-8",
        delimiter: str = ",",
    ) -> None:
        self.topic = topic
        self.csv_path = Path(csv_path)
        self.encoding = encoding
        self.delimiter = delimiter

        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def _read_csv(self) -> Iterable[Dict[str, Any]]:
        with self.csv_path.open("r", encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                yield row

    def send_all(self, limit: Optional[int] = None) -> None:
        """
        Gửi toàn bộ (hoặc `limit` bản ghi đầu tiên) của file CSV lên Kafka.
        """
        count = 0
        batch_count = 0

        for row in self._read_csv():
            if limit is not None and count >= limit:
                break

            self.producer.send(self.topic, value=row)
            count += 1
            batch_count += 1

            if PRODUCER_SLEEP_SECONDS > 0:
                time.sleep(PRODUCER_SLEEP_SECONDS)

            if batch_count >= PRODUCER_FLUSH_INTERVAL:
                self.producer.flush()
                batch_count = 0

        # Flush phần còn lại
        self.producer.flush()

        print(f"Đã gửi {count} bản ghi từ '{self.csv_path}' lên topic '{self.topic}'.")


