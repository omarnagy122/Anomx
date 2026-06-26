import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/cmapss")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_connect(client, userdata, flags, rc):
    print(f"[mqtt_bridge] Connected to MQTT broker — rc: {rc}")
    client.subscribe(MQTT_TOPIC)
    print(f"[mqtt_bridge] Subscribed to topic: {MQTT_TOPIC}")

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode('utf-8'))
    producer.send(KAFKA_TOPIC, value=data)
    print(f"[mqtt_bridge] Forwarded → Engine {data.get('engine_id')} | Cycle {data.get('time_in_cycles')} | Source {data.get('source')}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

print(f"[mqtt_bridge] Connecting to MQTT: {MQTT_HOST}:{MQTT_PORT}")
client.connect(MQTT_HOST, MQTT_PORT)
client.loop_forever()
