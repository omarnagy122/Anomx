from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_batch

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASS,
    SENSOR_COLUMNS, CONSUMER_TIMEOUT_MS, CONSUMER_LOG_EVERY, POSTGRES_BATCH_SIZE
)

RAW_INSERT_COLUMNS = ["source_file", *SENSOR_COLUMNS]

INSERT_SQL = f"""
INSERT INTO raw_sensor_data ({", ".join(RAW_INSERT_COLUMNS)})
VALUES ({", ".join([f"%({col})s" for col in RAW_INSERT_COLUMNS])})
ON CONFLICT (source_file, engine_id, time_in_cycles)
DO NOTHING;
"""

def _normalise_message(data: dict[str, Any]) -> dict[str, Any]:
    data.setdefault("source", "FD001")
    data.setdefault("source_file", data["source"])
    normalised = {col: data.get(col) for col in RAW_INSERT_COLUMNS}
    normalised["source_file"] = str(normalised["source_file"])
    normalised["engine_id"] = int(normalised["engine_id"])
    normalised["time_in_cycles"] = int(normalised["time_in_cycles"])
    return normalised

def _commit_batch(conn, consumer, cursor, batch: list, processed: int) -> None:
    if not batch:
        return
    execute_batch(cursor, INSERT_SQL, batch, page_size=len(batch))
    conn.commit()
    consumer.commit()
    print(f"[consumer] Committed {len(batch)} rows — total: {processed}")
    batch.clear()

def consume():
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS
    )
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    print(f"[consumer] Started — topic: {KAFKA_TOPIC}")

    processed = 0
    skipped = 0
    batch = []

    try:
        for message in consumer:
            try:
                data = _normalise_message(message.value)
            except Exception as exc:
                skipped += 1
                print(f"[consumer] Skipped invalid message: {exc}")
                continue

            batch.append(data)
            processed += 1

            if processed % CONSUMER_LOG_EVERY == 0:
                print(f"[consumer] Processed {processed} messages")

            if len(batch) >= POSTGRES_BATCH_SIZE:
                _commit_batch(conn, consumer, cursor, batch, processed)

        _commit_batch(conn, consumer, cursor, batch, processed)
        print(f"[consumer] Done — processed: {processed}, skipped: {skipped}")
        return processed, skipped

    except Exception:
        conn.rollback()
        print("[consumer] Rolled back transaction.")
        raise

    finally:
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    consume()
