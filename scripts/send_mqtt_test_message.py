from __future__ import annotations

import json
import os
import time

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/cmapss")
MQTT_QOS = int(os.getenv("MQTT_QOS", "0"))
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))

payload = {
    "source_file": os.getenv("TEST_MACHINE_SOURCE", "REAL_MACHINE_TEST"),
    "engine_id": int(os.getenv("TEST_MACHINE_ENGINE_ID", "9001")),
    "time_in_cycles": int(os.getenv("TEST_MACHINE_CYCLE", str(int(time.time())))),
    "op_setting_1": 0.0,
    "op_setting_2": 0.0,
    "op_setting_3": 100.0,
    "s2": 641.82,
    "s3": 1589.70,
    "s4": 1400.60,
    "s7": 554.36,
    "s11": 47.47,
    "s12": 521.66,
    "s15": 8.4195,
    "s20": 39.06,
    "s21": 23.419,
}

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="anomx-machine-test")
client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
client.loop_start()
info = client.publish(MQTT_TOPIC, payload=json.dumps(payload, separators=(",", ":")), qos=MQTT_QOS)
info.wait_for_publish()
client.loop_stop()
client.disconnect()

if info.rc != mqtt.MQTT_ERR_SUCCESS:
    raise SystemExit(f"MQTT publish failed with rc={info.rc}")

print(f"[machine-mqtt-test] Published one reading to {MQTT_HOST}:{MQTT_PORT}/{MQTT_TOPIC}: {payload}")
