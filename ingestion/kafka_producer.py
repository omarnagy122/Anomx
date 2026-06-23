from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import (  # noqa: E402
    DATA_SOURCES,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MODE,
    SENSOR_COLUMNS,
)


def _resolve_dataset_path(dataset: str) -> Path:
    env_path = os.getenv("ANOMX_DATASET_PATH")
    if env_path:
        return Path(env_path)

    try:
        return Path(DATA_SOURCES["simulation"][dataset])
    except KeyError as exc:
        raise ValueError(f"Unknown dataset '{dataset}'. Expected one of FD001, FD002, FD003, FD004.") from exc


def stream_simulation(dataset: str = "FD001") -> int:
    dataset_path = _resolve_dataset_path(dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset file not found: {dataset_path}")

    sleep_seconds = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.1"))
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TOPIC", KAFKA_TOPIC)

    df = pd.read_csv(dataset_path, sep=r"\s+", header=None, names=SENSOR_COLUMNS)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    print(f"[producer] Dataset: {dataset}")
    print(f"[producer] Path: {dataset_path}")
    print(f"[producer] Kafka: {bootstrap_servers}")
    print(f"[producer] Topic: {topic}")
    print(f"[producer] Streaming {len(df)} rows...")

    for _, row in df.iterrows():
        message = row.to_dict()
        message["source"] = dataset
        message["source_file"] = dataset
        producer.send(topic, value=message)
        print(
            f"Sent -> Engine {int(message['engine_id'])} | "
            f"Cycle {int(message['time_in_cycles'])} | Source {dataset}"
        )
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    producer.flush()
    producer.close()
    print(f"[producer] Done — {dataset} streamed successfully.")
    return len(df)


def stream_live() -> int:
    print("[live] MQTT streaming is not implemented yet.")
    print("[live] Set MQTT_HOST, MQTT_PORT, and MQTT_TOPIC when real sensors are available.")
    return 0


if __name__ == "__main__":
    dataset_arg = sys.argv[1] if len(sys.argv) > 1 else os.getenv("ANOMX_DATASET", "FD001")

    if MODE == "simulation":
        stream_simulation(dataset_arg)
    elif MODE == "live":
        stream_live()
    else:
        raise ValueError(f"Unsupported ANOMX_MODE: {MODE}")
