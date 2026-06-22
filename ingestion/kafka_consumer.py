
from kafka import KafkaConsumer
import psycopg2
import json
import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomx-group-v4")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomx_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "anomx")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "anomx123")


print(f"Connecting to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)

cursor = conn.cursor()

print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topic: {KAFKA_TOPIC}")
print(f"Group ID: {KAFKA_GROUP_ID}")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id=KAFKA_GROUP_ID,
    enable_auto_commit=True,
    consumer_timeout_ms=30000,
)

print("Consumer started — waiting for messages...")

inserted_count = 0
skipped_count = 0

try:
    for message in consumer:
        raw_value = message.value

        try:
            decoded_value = raw_value.decode("utf-8")
            data = json.loads(decoded_value)
        except Exception:
            skipped_count += 1
            print(f"Skipped non-JSON message: {raw_value}")
            continue

        cursor.execute(
            """
            INSERT INTO raw_sensor_data (
                engine_id, time_in_cycles,
                op_setting_1, op_setting_2, op_setting_3,
                s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
                s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21
            ) VALUES (
                %(engine_id)s, %(time_in_cycles)s,
                %(op_setting_1)s, %(op_setting_2)s, %(op_setting_3)s,
                %(s1)s, %(s2)s, %(s3)s, %(s4)s, %(s5)s,
                %(s6)s, %(s7)s, %(s8)s, %(s9)s, %(s10)s,
                %(s11)s, %(s12)s, %(s13)s, %(s14)s, %(s15)s,
                %(s16)s, %(s17)s, %(s18)s, %(s19)s, %(s20)s, %(s21)s
            )
            """,
            data,
        )

        inserted_count += 1

        if inserted_count % 100 == 0:
            conn.commit()
            print(f"Committed {inserted_count} rows so far...")

        print(f"Saved -> Engine {data['engine_id']} | Cycle {data['time_in_cycles']}")

    conn.commit()

    print("Consumer finished.")
    print(f"Total inserted rows: {inserted_count}")
    print(f"Total skipped non-JSON messages: {skipped_count}")

except Exception as exc:
    conn.rollback()
    print(f"Consumer failed. Rolled back current transaction. Error: {exc}")
    raise

finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("Kafka consumer and PostgreSQL connection closed.")
