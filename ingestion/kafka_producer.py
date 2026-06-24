import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from kafka import KafkaProducer
import json
import time
from config import (
    MODE, DATA_SOURCES,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    SENSOR_COLUMNS
)

def stream_simulation(dataset="FD001"):
    path = DATA_SOURCES["simulation"][dataset]
    df = pd.read_csv(path, sep=r'\s+', header=None, names=SENSOR_COLUMNS)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print(f"[simulation] Streaming {dataset} — {len(df)} rows...")

    for _, row in df.iterrows():
        message = row.to_dict()
        message['source'] = dataset
        producer.send(KAFKA_TOPIC, value=message)
        print(f"Sent → Engine {int(message['engine_id'])} | Cycle {int(message['time_in_cycles'])}")
        time.sleep(0.01)

    producer.flush()
    print(f"[simulation] Done — {dataset} streamed successfully.")

def stream_live():
    # TODO: implement MQTT connection when real sensors are available
    print("[live] MQTT streaming not yet implemented.")
    print("[live] Set MQTT_HOST, MQTT_PORT, MQTT_TOPIC env variables when ready.")

if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else "FD001"

    if MODE == "simulation":
        stream_simulation(dataset)
    elif MODE == "live":
        stream_live()
