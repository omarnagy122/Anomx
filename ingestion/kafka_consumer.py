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

from config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SENSOR_COLUMNS,
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT 'FD001',
    engine_id INTEGER NOT NULL,
    time_in_cycles INTEGER NOT NULL,
    op_setting_1 DOUBLE PRECISION,
    op_setting_2 DOUBLE PRECISION,
    op_setting_3 DOUBLE PRECISION,
    s1 DOUBLE PRECISION,
    s2 DOUBLE PRECISION,
    s3 DOUBLE PRECISION,
    s4 DOUBLE PRECISION,
    s5 DOUBLE PRECISION,
    s6 DOUBLE PRECISION,
    s7 DOUBLE PRECISION,
    s8 DOUBLE PRECISION,
    s9 DOUBLE PRECISION,
    s10 DOUBLE PRECISION,
    s11 DOUBLE PRECISION,
    s12 DOUBLE PRECISION,
    s13 DOUBLE PRECISION,
    s14 DOUBLE PRECISION,
    s15 DOUBLE PRECISION,
    s16 DOUBLE PRECISION,
    s17 DOUBLE PRECISION,
    s18 DOUBLE PRECISION,
    s19 DOUBLE PRECISION,
    s20 DOUBLE PRECISION,
    s21 DOUBLE PRECISION,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT raw_sensor_data_unique_row UNIQUE (source_file, engine_id, time_in_cycles)
);
"""

RAW_INSERT_COLUMNS = ["source_file", *SENSOR_COLUMNS]
INSERT_SQL = f"""
INSERT INTO raw_sensor_data ({", ".join(RAW_INSERT_COLUMNS)})
VALUES ({", ".join([f"%({column})s" for column in RAW_INSERT_COLUMNS])})
ON CONFLICT (source_file, engine_id, time_in_cycles)
DO NOTHING;
"""


def _normalise_message(data: dict[str, Any]) -> dict[str, Any]:
    data.setdefault("source", "FD001")
    data.setdefault("source_file", data["source"])

    normalised = {column: data.get(column) for column in RAW_INSERT_COLUMNS}
    normalised["source_file"] = str(normalised["source_file"])
    normalised["engine_id"] = int(normalised["engine_id"])
    normalised["time_in_cycles"] = int(normalised["time_in_cycles"])
    return normalised


def _commit_batch(conn, consumer, cursor, batch: list[dict[str, Any]], processed: int) -> None:
    if not batch:
        return
    execute_batch(cursor, INSERT_SQL, batch, page_size=len(batch))
    conn.commit()
    # Commit Kafka offsets only after PostgreSQL commit succeeds.
    consumer.commit()
    print(f"[consumer] PostgreSQL + Kafka offsets committed at {processed} processed messages.")
    batch.clear()


def consume() -> tuple[int, int]:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TOPIC", KAFKA_TOPIC)
    group_id = os.getenv("KAFKA_GROUP_ID", KAFKA_GROUP_ID)
    consumer_timeout_ms = int(os.getenv("CONSUMER_TIMEOUT_MS", "30000"))
    batch_size = int(os.getenv("POSTGRES_BATCH_SIZE", "500"))
    log_every = int(os.getenv("CONSUMER_LOG_EVERY", "1000"))

    print(f"[consumer] Connecting PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()

    print(f"[consumer] Connecting Kafka: {bootstrap_servers}")
    print(f"[consumer] Topic: {topic}")
    print(f"[consumer] Group ID: {group_id}")
    print("[consumer] Kafka auto offset commit: disabled. Offsets commit only after PostgreSQL commit.")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=group_id,
        enable_auto_commit=False,
        consumer_timeout_ms=consumer_timeout_ms,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    processed = 0
    skipped = 0
    batch: list[dict[str, Any]] = []

    try:
        for message in consumer:
            try:
                data = _normalise_message(message.value)
            except Exception as exc:
                skipped += 1
                print(f"[consumer] Skipped invalid message at offset {message.offset}: {exc}")
                continue

            batch.append(data)
            processed += 1

            if processed == 1 or processed % log_every == 0:
                print(
                    f"[consumer] Processed {processed} -> "
                    f"Engine {data['engine_id']} | Cycle {data['time_in_cycles']} | Source {data['source_file']}"
                )

            if len(batch) >= batch_size:
                _commit_batch(conn, consumer, cursor, batch, processed)

        _commit_batch(conn, consumer, cursor, batch, processed)
        print("[consumer] Finished.")
        print(f"[consumer] Processed messages: {processed}")
        print(f"[consumer] Skipped messages: {skipped}")
        return processed, skipped

    except Exception:
        conn.rollback()
        print("[consumer] Rolled back PostgreSQL transaction. Kafka offsets were not committed for failed batch.")
        raise

    finally:
        consumer.close()
        cursor.close()
        conn.close()
        print("[consumer] Kafka consumer and PostgreSQL connection closed.")


if __name__ == "__main__":
    consume()
