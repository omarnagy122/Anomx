from kafka import KafkaConsumer
import psycopg2
import json

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="anomx_db",
    user="anomx",
    password="anomx123"
)
cursor = conn.cursor()

# Kafka Consumer
consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
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
            s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21
        ) VALUES (
            %(engine_id)s, %(time_in_cycles)s,
            %(op_setting_1)s, %(op_setting_2)s, %(op_setting_3)s,
            %(s1)s, %(s2)s, %(s3)s, %(s4)s, %(s5)s,
            %(s6)s, %(s7)s, %(s8)s, %(s9)s, %(s10)s,
            %(s11)s, %(s12)s, %(s13)s, %(s14)s, %(s15)s,
            %(s16)s, %(s17)s, %(s18)s, %(s19)s, %(s20)s, %(s21)s
        )
    """, data)

    conn.commit()
    print(f"Saved → Engine {data['engine_id']} | Cycle {data['time_in_cycles']}")

