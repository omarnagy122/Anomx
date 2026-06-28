from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_LOG_EVERY,
    PRODUCER_SLEEP_SECONDS,
    SENSOR_COLUMNS,
    dataset_path,
)


def _json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _record_to_message(record: dict[str, Any], dataset: str) -> dict[str, Any]:
    message = {key: _json_safe(value) for key, value in record.items()}
    message["source"] = dataset
    message["source_file"] = dataset
    return message


def _slice_rows(df: pd.DataFrame, start_row: int, limit: int | None) -> pd.DataFrame:
    """Return a 1-based row slice for realistic incremental sensor simulation."""
    if start_row < 1:
        raise ValueError("--start-row must be >= 1")
    start_index = start_row - 1
    if limit is None:
        return df.iloc[start_index:].copy()
    if limit < 0:
        raise ValueError("--limit must be >= 0")
    return df.iloc[start_index : start_index + limit].copy()


def stream_simulation(
    dataset: str = "FD001",
    *,
    limit: int | None = None,
    start_row: int = 1,
    sleep_seconds: float | None = None,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
) -> int:
    """Simulate real-time sensor readings by sending C-MAPSS rows to Kafka.

    This script only produces raw readings. Cleaning, feature engineering, and
    prediction are intentionally handled later by the scheduled prediction flow.

    ``start_row`` is 1-based. It is useful when testing incremental processing:
    first send ``--start-row 1 --limit 1000``, then send
    ``--start-row 1001 --limit 500`` to generate only new readings.
    """

    dataset = dataset.upper()
    path = dataset_path(dataset)
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset file not found: {path}. Copy train_{dataset}.txt into data/raw/CMAPSSData first."
        )

    sleep_seconds = PRODUCER_SLEEP_SECONDS if sleep_seconds is None else sleep_seconds
    bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic = topic or os.getenv("KAFKA_TOPIC", KAFKA_TOPIC)
    log_every = int(os.getenv("PRODUCER_LOG_EVERY", str(PRODUCER_LOG_EVERY)))

    source_df = pd.read_csv(path, sep=r"\s+", header=None, names=SENSOR_COLUMNS)
    df = _slice_rows(source_df, start_row=start_row, limit=limit)

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x, allow_nan=False).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent_count = 0
    end_row = start_row + len(df) - 1 if len(df) else start_row - 1
    print(f"[producer] Dataset: {dataset}")
    print(f"[producer] Path: {path}")
    print(f"[producer] Kafka: {bootstrap_servers}")
    print(f"[producer] Topic: {topic}")
    print(f"[producer] Source rows selected: {start_row}..{end_row}")
    print(f"[producer] Rows to stream: {len(df)}")

    try:
        for record in df.to_dict("records"):
            message = _record_to_message(record, dataset)
            key = f"{dataset}:{int(message['engine_id'])}:{int(message['time_in_cycles'])}"
            producer.send(topic, key=key, value=message)
            sent_count += 1

            if sent_count == 1 or sent_count % log_every == 0 or sent_count == len(df):
                print(
                    f"[producer] Sent {sent_count}/{len(df)} -> "
                    f"Engine {int(message['engine_id'])} | Cycle {int(message['time_in_cycles'])} | Source {dataset}"
                )

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        producer.flush()
    finally:
        producer.close()

    print(f"[producer] Done — {sent_count} raw readings streamed.")
    return sent_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream C-MAPSS rows to Kafka as raw sensor readings.")
    parser.add_argument("dataset", nargs="?", default=os.getenv("ANOMX_DATASET", "FD001"))
    parser.add_argument("--limit", type=int, default=None, help="Optional number of rows to stream from start-row.")
    parser.add_argument("--start-row", type=int, default=1, help="1-based source file row to start streaming from.")
    parser.add_argument("--sleep", type=float, default=None, help="Seconds between rows. Default from PRODUCER_SLEEP_SECONDS.")
    args = parser.parse_args()
    stream_simulation(args.dataset, limit=args.limit, start_row=args.start_row, sleep_seconds=args.sleep)


if __name__ == "__main__":
    main()
