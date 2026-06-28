from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (  # noqa: E402
    CONSUMER_LOG_EVERY,
    CONSUMER_TIMEOUT_MS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
    POSTGRES_COMMIT_EVERY,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SENSOR_COLUMNS,
)

CREATE_RAW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT 'UNKNOWN',
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
INSERT_RAW_SQL = f"""
INSERT INTO raw_sensor_data ({", ".join(RAW_INSERT_COLUMNS)})
VALUES ({", ".join([f"%({column})s" for column in RAW_INSERT_COLUMNS])})
ON CONFLICT (source_file, engine_id, time_in_cycles)
DO NOTHING;
"""


def _connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", POSTGRES_HOST),
        port=int(os.getenv("POSTGRES_PORT", str(POSTGRES_PORT))),
        database=os.getenv("POSTGRES_DB", POSTGRES_DB),
        user=os.getenv("POSTGRES_USER", POSTGRES_USER),
        password=os.getenv("POSTGRES_PASSWORD", POSTGRES_PASSWORD),
    )


def _normalise_message(data: dict[str, Any]) -> dict[str, Any]:
    source = data.get("source_file") or data.get("source") or "UNKNOWN"
    normalised = {column: data.get(column) for column in SENSOR_COLUMNS}
    normalised["source_file"] = str(source)
    normalised["engine_id"] = int(normalised["engine_id"])
    normalised["time_in_cycles"] = int(normalised["time_in_cycles"])
    return normalised


def consume() -> tuple[int, int, int, int]:
    """Consume Kafka messages row-by-row and store raw readings only.

    This is the real-time ingestion path. It deliberately does not import heavy processing modules,
    does not clean data, and does not run predictions.
    """

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TOPIC", KAFKA_TOPIC)
    group_id = os.getenv("KAFKA_GROUP_ID", KAFKA_GROUP_ID)
    consumer_timeout_ms = int(os.getenv("CONSUMER_TIMEOUT_MS", str(CONSUMER_TIMEOUT_MS)))
    commit_every = max(1, int(os.getenv("POSTGRES_COMMIT_EVERY", str(POSTGRES_COMMIT_EVERY))))
    log_every = max(1, int(os.getenv("CONSUMER_LOG_EVERY", str(CONSUMER_LOG_EVERY))))

    print(f"[consumer] Real-time raw storage mode: Kafka -> raw_sensor_data only.")
    print(f"[consumer] Kafka: {bootstrap_servers} | topic={topic} | group={group_id}")
    print(f"[consumer] PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"[consumer] Commit policy: every {commit_every} row(s). Default is 1 for row-by-row real-time ingestion.")

    conn = _connect_postgres()
    cursor = conn.cursor()
    cursor.execute(CREATE_RAW_TABLE_SQL)
    conn.commit()

    consumer_kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "auto_offset_reset": "earliest",
        "group_id": group_id,
        "enable_auto_commit": False,
        "value_deserializer": lambda raw: json.loads(raw.decode("utf-8")),
    }
    if consumer_timeout_ms > 0:
        consumer_kwargs["consumer_timeout_ms"] = consumer_timeout_ms

    consumer = KafkaConsumer(topic, **consumer_kwargs)

    consumed = 0
    inserted = 0
    duplicates = 0
    skipped = 0
    uncommitted_rows = 0

    try:
        for message in consumer:
            try:
                data = _normalise_message(message.value)
                cursor.execute(INSERT_RAW_SQL, data)
                consumed += 1
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1
                uncommitted_rows += 1
            except Exception as exc:
                skipped += 1
                print(f"[consumer] Skipped invalid message at offset {getattr(message, 'offset', '?')}: {exc}")
                continue

            if uncommitted_rows >= commit_every:
                conn.commit()
                consumer.commit()
                uncommitted_rows = 0

            if consumed == 1 or consumed % log_every == 0:
                print(
                    f"[consumer] Consumed={consumed} inserted={inserted} duplicates={duplicates} -> "
                    f"Engine {data['engine_id']} | Cycle {data['time_in_cycles']} | Source {data['source_file']}"
                )

        if uncommitted_rows:
            conn.commit()
            consumer.commit()

        print(f"[consumer] Finished. consumed={consumed}, inserted={inserted}, duplicates={duplicates}, skipped={skipped}")
        return consumed, inserted, duplicates, skipped

    except Exception:
        conn.rollback()
        print("[consumer] Rolled back failed PostgreSQL transaction. Kafka offsets for failed rows were not committed.")
        raise
    finally:
        consumer.close()
        cursor.close()
        conn.close()
        print("[consumer] Closed Kafka consumer and PostgreSQL connection.")


if __name__ == "__main__":
    consume()
