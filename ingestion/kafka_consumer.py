import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from kafka import KafkaConsumer
import psycopg2
import json
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    POSTGRES_HOST, POSTGRES_PORT,
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASS
)

def consume():
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS
    )
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='anomx-group'
    )

    print("Consumer started — waiting for messages...")

    for message in consumer:
        data = message.value
        cursor.execute("""
            INSERT INTO raw_sensor_data (
                engine_id, time_in_cycles,
                op_setting_1, op_setting_2, op_setting_3,
                s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
                s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21,
                source_file
            ) VALUES (
                %(engine_id)s, %(time_in_cycles)s,
                %(op_setting_1)s, %(op_setting_2)s, %(op_setting_3)s,
                %(s1)s, %(s2)s, %(s3)s, %(s4)s, %(s5)s,
                %(s6)s, %(s7)s, %(s8)s, %(s9)s, %(s10)s,
                %(s11)s, %(s12)s, %(s13)s, %(s14)s, %(s15)s,
                %(s16)s, %(s17)s, %(s18)s, %(s19)s, %(s20)s, %(s21)s,
                %(source)s
            )
            ON CONFLICT (source_file, engine_id, time_in_cycles)
            DO NOTHING
        """, data)
        conn.commit()
        print(f"Saved → Engine {data['engine_id']} | Cycle {data['time_in_cycles']} | Source {data['source']}")

if __name__ == "__main__":
    consume()
