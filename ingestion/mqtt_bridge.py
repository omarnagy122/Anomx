from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_KEEPALIVE,
    MQTT_PORT,
    MQTT_QOS,
    MQTT_TOPIC,
    PRODUCER_LOG_EVERY,
    SENSOR_COLUMNS,
)

REQUIRED_MESSAGE_COLUMNS = {"engine_id", "time_in_cycles"}
FORWARDED = 0
SKIPPED = 0
SHUTDOWN = False


def _mqtt_client(client_id: str) -> mqtt.Client:
    """Create a Paho client using callback API v2 to avoid v1/v2 runtime errors."""
    return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)


def _json_safe_loads(payload: bytes) -> dict[str, Any]:
    value = json.loads(payload.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("MQTT payload must be a JSON object.")
    return value


def _normalise_machine_message(data: dict[str, Any]) -> dict[str, Any]:
    missing = REQUIRED_MESSAGE_COLUMNS.difference(data)
    if missing:
        raise ValueError(f"Missing required field(s): {', '.join(sorted(missing))}")

    source = data.get("source_file") or data.get("source") or "MQTT"
    message = {column: data.get(column) for column in SENSOR_COLUMNS}
    message["source"] = str(source)
    message["source_file"] = str(source)
    message["engine_id"] = int(message["engine_id"])
    message["time_in_cycles"] = int(message["time_in_cycles"])
    return message


def _kafka_key(message: dict[str, Any]) -> str:
    return f"{message['source_file']}:{message['engine_id']}:{message['time_in_cycles']}"


def build_kafka_producer() -> KafkaProducer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    print(f"[mqtt_bridge] Kafka: {bootstrap_servers} | topic={os.getenv('KAFKA_TOPIC', KAFKA_TOPIC)}")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x, allow_nan=False).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8"),
        acks="all",
        retries=5,
    )


def on_connect(client: mqtt.Client, userdata: dict[str, Any], flags: mqtt.ConnectFlags, reason_code: mqtt.ReasonCode, properties: mqtt.Properties | None) -> None:
    if reason_code.is_failure:
        print(f"[mqtt_bridge] MQTT connection failed: {reason_code}")
        return
    topic = userdata["mqtt_topic"]
    qos = userdata["mqtt_qos"]
    client.subscribe(topic, qos=qos)
    print(f"[mqtt_bridge] Connected to MQTT broker and subscribed to {topic} with qos={qos}")


def on_message(client: mqtt.Client, userdata: dict[str, Any], msg: mqtt.MQTTMessage) -> None:
    global FORWARDED, SKIPPED
    producer: KafkaProducer = userdata["producer"]
    kafka_topic = userdata["kafka_topic"]
    log_every = userdata["log_every"]

    try:
        raw = _json_safe_loads(msg.payload)
        message = _normalise_machine_message(raw)
        producer.send(kafka_topic, key=_kafka_key(message), value=message)
        FORWARDED += 1
        if FORWARDED == 1 or FORWARDED % log_every == 0:
            producer.flush()
            print(
                f"[mqtt_bridge] Forwarded={FORWARDED} skipped={SKIPPED} -> "
                f"Engine {message['engine_id']} | Cycle {message['time_in_cycles']} | Source {message['source_file']}"
            )
    except Exception as exc:
        SKIPPED += 1
        print(f"[mqtt_bridge] Skipped invalid MQTT message on {msg.topic}: {exc}")


def _handle_signal(signum: int, frame: Any) -> None:
    global SHUTDOWN
    SHUTDOWN = True
    print(f"[mqtt_bridge] Received signal {signum}; shutting down...")


def run_bridge() -> tuple[int, int]:
    mqtt_host = os.getenv("MQTT_HOST", MQTT_HOST)
    mqtt_port = int(os.getenv("MQTT_PORT", str(MQTT_PORT)))
    mqtt_topic = os.getenv("MQTT_TOPIC", MQTT_TOPIC)
    mqtt_qos = int(os.getenv("MQTT_QOS", str(MQTT_QOS)))
    kafka_topic = os.getenv("KAFKA_TOPIC", KAFKA_TOPIC)
    log_every = max(1, int(os.getenv("PRODUCER_LOG_EVERY", str(PRODUCER_LOG_EVERY))))

    producer = build_kafka_producer()
    client_id = f"{MQTT_CLIENT_ID}-bridge-{os.getpid()}"
    client = _mqtt_client(client_id)
    client.user_data_set(
        {
            "producer": producer,
            "mqtt_topic": mqtt_topic,
            "mqtt_qos": mqtt_qos,
            "kafka_topic": kafka_topic,
            "log_every": log_every,
        }
    )
    client.on_connect = on_connect
    client.on_message = on_message

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    print(f"[mqtt_bridge] MQTT broker: {mqtt_host}:{mqtt_port} | topic={mqtt_topic}")
    try:
        client.connect(mqtt_host, mqtt_port, keepalive=MQTT_KEEPALIVE)
        client.loop_start()
        while not SHUTDOWN:
            time.sleep(1)
    finally:
        client.loop_stop()
        client.disconnect()
        producer.flush()
        producer.close()
        print(f"[mqtt_bridge] Closed. forwarded={FORWARDED}, skipped={SKIPPED}")
    return FORWARDED, SKIPPED


if __name__ == "__main__":
    run_bridge()
