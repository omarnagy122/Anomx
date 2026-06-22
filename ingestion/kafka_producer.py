import pandas as pd
from kafka import KafkaProducer
import json
import time

# Column names for C-MAPSS dataset
COLUMNS = [
    'engine_id', 'time_in_cycles',
    'op_setting_1', 'op_setting_2', 'op_setting_3',
    's1','s2','s3','s4','s5','s6','s7','s8','s9','s10',
    's11','s12','s13','s14','s15','s16','s17','s18','s19','s20','s21'
]

# Load data
df = pd.read_csv(
    "/opt/airflow/data/raw/CMAPSSData/train_FD001.txt",
    sep=r"\s+",
    header=None,
    names=COLUMNS
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='host.docker.internal:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print(f"Starting to stream {len(df)} rows...")

for _, row in df.iterrows():
    message = row.to_dict()

    producer.send(
        'sensor-data',
        value=message
    )

    print(
        f"Sent -> Engine {int(message['engine_id'])} | "
        f"Cycle {int(message['time_in_cycles'])}"
    )

    time.sleep(0.1)

producer.flush()

print("Done!")
