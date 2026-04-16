from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable

from confluent_kafka import Producer
from dotenv import load_dotenv

from ingestion.collection import collector
from ingestion.collection.normalize import normalize_payload


DEFAULT_RAW_TOPIC = "live-raw"
DEFAULT_EVENT_TOPIC = "live-events"
DEFAULT_POLL_INTERVAL = 10
DEFAULT_RAW_DIR = Path("data/live/raw")
DEFAULT_EVENT_DIR = Path("data/live/normalized")


def _load_env() -> None:
    load_dotenv()
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    if repo_env.exists():
        load_dotenv(repo_env, override=False)
    collection_env = Path(__file__).resolve().parents[1] / "collection" / ".env"
    if collection_env.exists():
        load_dotenv(collection_env, override=False)


def _write_jsonl(records: Iterable[Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def publish_to_kafka(producer: Producer, topic: str, data: Any) -> None:
    producer.produce(topic, value=json.dumps(data))


def main() -> None:
    _load_env()

    kafka_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    }
    producer = Producer(kafka_config)

    raw_topic = os.getenv("KAFKA_RAW_TOPIC", DEFAULT_RAW_TOPIC)
    event_topic = os.getenv("KAFKA_EVENT_TOPIC", DEFAULT_EVENT_TOPIC)
    poll_interval = float(os.getenv("API_POLL_INTERVAL", str(DEFAULT_POLL_INTERVAL)))

    raw_dir = Path(os.getenv("RAW_OUT_DIR", str(DEFAULT_RAW_DIR)))
    event_dir = Path(os.getenv("EVENT_OUT_DIR", str(DEFAULT_EVENT_DIR)))

    while True:
        payload = collector.fetch()
        if payload:
            publish_to_kafka(producer, raw_topic, payload)
            _write_jsonl([payload], raw_dir / "raw.jsonl")

            events = normalize_payload(payload)
            if events:
                for event in events:
                    publish_to_kafka(producer, event_topic, event)
                _write_jsonl(events, event_dir / "events.jsonl")

            producer.flush()

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
