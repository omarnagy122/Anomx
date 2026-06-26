import sys
import os
import time
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import paho.mqtt.client as mqtt
from config import (
    DATA_SOURCES, SENSOR_COLUMNS,
    PRODUCER_SLEEP_SECONDS, PRODUCER_LOG_EVERY
)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/cmapss")

def simulate(dataset="FD001"):
    path = DATA_SOURCES["simulation"][dataset]
    df = pd.read_csv(path, sep=r'\s+', header=None, names=SENSOR_COLUMNS)

    client = mqtt.Client()
    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_start()

    print(f"[mqtt_simulator] Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
    print(f"[mqtt_simulator] Streaming {dataset} — {len(df)} rows...")

    for i, (_, row) in enumerate(df.iterrows()):
        message = row.to_dict()
        message['source'] = dataset
        payload = json.dumps(message)
        client.publish(MQTT_TOPIC, payload)

        if i % PRODUCER_LOG_EVERY == 0:
            print(f"[mqtt_simulator] Sent {i} rows — Engine {int(message['engine_id'])} | Cycle {int(message['time_in_cycles'])}")

        time.sleep(PRODUCER_SLEEP_SECONDS)

    client.loop_stop()
    client.disconnect()
    print(f"[mqtt_simulator] Done — {dataset} streamed successfully.")

if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else "FD001"
    simulate(dataset)
