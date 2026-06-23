from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg2
from kafka import KafkaConsumer

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

INSERT_SQL = """
INSERT INTO raw_sensor_data (
    source_file, engine_id, time_in_cycles,
    op_setting_1, op_setting_2, op_setting_3,
    s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
    s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21
) VALUES (
    %(source_file)s, %(engine_id)s, %(time_in_cycles)s,
    %(op_setting_1)s, %(op_setting_2)s, %(op_setting_3)s,
    %(s1)s, %(s2)s, %(s3)s, %(s4)s, %(s5)s, %(s6)s, %(s7)s, %(s8)s, %(s9)s, %(s10)s,
    %(s11)s, %(s12)s, %(s13)s, %(s14)s, %(s15)s, %(s16)s, %(s17)s, %(s18)s, %(s19)s, %(s20)s, %(s21)s
)
ON CONFLICT (source_file, engine_id, time_in_cycles)
DO NOTHING;
"""


def consume() -> tuple[int, int]:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TOPIC", KAFKA_TOPIC)
    group_id = os.getenv("KAFKA_GROUP_ID", KAFKA_GROUP_ID)
    consumer_timeout_ms = int(os.getenv("CONSUMER_TIMEOUT_MS", "30000"))

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

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=group_id,
        enable_auto_commit=True,
        consumer_timeout_ms=consumer_timeout_ms,
    )

    inserted_or_seen = 0
    skipped = 0

    try:
        for message in consumer:
            try:
                data = json.loads(message.value.decode("utf-8"))
            except Exception:
                skipped += 1
                print(f"[consumer] Skipped non-JSON message: {message.value!r}")
                continue

            data.setdefault("source", "FD001")
            data.setdefault("source_file", data["source"])

            cursor.execute(INSERT_SQL, data)
            inserted_or_seen += 1

            if inserted_or_seen % 100 == 0:
                conn.commit()
                print(f"[consumer] Processed {inserted_or_seen} messages so far...")

            print(
                f"Saved/ignored duplicate -> Engine {data['engine_id']} | "
                f"Cycle {data['time_in_cycles']} | Source {data['source_file']}"
            )

        conn.commit()
        print("[consumer] Finished.")
        print(f"[consumer] Processed messages: {inserted_or_seen}")
        print(f"[consumer] Skipped messages: {skipped}")
        return inserted_or_seen, skipped

    except Exception:
        conn.rollback()
        raise

    finally:
        consumer.close()
        cursor.close()
        conn.close()
        print("[consumer] Kafka consumer and PostgreSQL connection closed.")


if __name__ == "__main__":
    consume()
