from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (  # noqa: E402
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_KEEPALIVE,
    MQTT_PORT,
    MQTT_QOS,
    MQTT_TOPIC,
    PRODUCER_LOG_EVERY,
    PRODUCER_SLEEP_SECONDS,
    SENSOR_COLUMNS,
    dataset_path,
)
from ingestion.kafka_producer import _record_to_message, _slice_rows  # noqa: E402


def _mqtt_client(client_id: str) -> mqtt.Client:
    """Create a Paho client using the v2 callback API.

    Omar's first MQTT files used the older callback style. paho-mqtt 2.x raises
    callback-version errors unless the API version and callback signatures match.
    """
    return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)


def _json_payload(message: dict[str, Any]) -> str:
    return json.dumps(message, allow_nan=False, separators=(",", ":"))


def simulate(
    dataset: str = "FD001",
    *,
    limit: int | None = None,
    start_row: int = 1,
    sleep_seconds: float | None = None,
    mqtt_host: str | None = None,
    mqtt_port: int | None = None,
    mqtt_topic: str | None = None,
    mqtt_qos: int | None = None,
) -> int:
    """Publish C-MAPSS rows to MQTT as if they came from live machine sensors."""

    dataset = dataset.upper()
    path = dataset_path(dataset)
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset file not found: {path}. Copy train_{dataset}.txt into data/raw/CMAPSSData first."
        )

    host = mqtt_host or os.getenv("MQTT_HOST", MQTT_HOST)
    port = int(mqtt_port or os.getenv("MQTT_PORT", str(MQTT_PORT)))
    topic = mqtt_topic or os.getenv("MQTT_TOPIC", MQTT_TOPIC)
    qos = int(mqtt_qos if mqtt_qos is not None else os.getenv("MQTT_QOS", str(MQTT_QOS)))
    sleep_seconds = PRODUCER_SLEEP_SECONDS if sleep_seconds is None else sleep_seconds
    log_every = max(1, int(os.getenv("PRODUCER_LOG_EVERY", str(PRODUCER_LOG_EVERY))))

    source_df = pd.read_csv(path, sep=r"\s+", header=None, names=SENSOR_COLUMNS)
    df = _slice_rows(source_df, start_row=start_row, limit=limit)

    client_id = f"{MQTT_CLIENT_ID}-simulator-{os.getpid()}"
    client = _mqtt_client(client_id)

    published = 0
    end_row = start_row + len(df) - 1 if len(df) else start_row - 1
    print(f"[mqtt_simulator] Dataset: {dataset}")
    print(f"[mqtt_simulator] Path: {path}")
    print(f"[mqtt_simulator] MQTT broker: {host}:{port}")
    print(f"[mqtt_simulator] MQTT topic: {topic}")
    print(f"[mqtt_simulator] Source rows selected: {start_row}..{end_row}")
    print(f"[mqtt_simulator] Rows to publish: {len(df)}")

    try:
        client.connect(host, port, keepalive=MQTT_KEEPALIVE)
        client.loop_start()
        for record in df.to_dict("records"):
            message = _record_to_message(record, dataset)
            payload = _json_payload(message)
            info = client.publish(topic, payload=payload, qos=qos)
            info.wait_for_publish()
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed with rc={info.rc}")

            published += 1
            if published == 1 or published % log_every == 0 or published == len(df):
                print(
                    f"[mqtt_simulator] Published {published}/{len(df)} -> "
                    f"Engine {int(message['engine_id'])} | Cycle {int(message['time_in_cycles'])} | Source {dataset}"
                )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    finally:
        client.loop_stop()
        client.disconnect()

    print(f"[mqtt_simulator] Done — {published} machine readings published to MQTT.")
    return published


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish C-MAPSS rows to MQTT as simulated machine telemetry.")
    parser.add_argument("dataset", nargs="?", default=os.getenv("ANOMX_DATASET", "FD001"))
    parser.add_argument("--limit", type=int, default=None, help="Optional number of rows to publish from start-row.")
    parser.add_argument("--start-row", type=int, default=1, help="1-based source file row to start publishing from.")
    parser.add_argument("--sleep", type=float, default=None, help="Seconds between rows. Default from PRODUCER_SLEEP_SECONDS.")
    parser.add_argument("--mqtt-host", default=None)
    parser.add_argument("--mqtt-port", type=int, default=None)
    parser.add_argument("--mqtt-topic", default=None)
    parser.add_argument("--mqtt-qos", type=int, default=None)
    args = parser.parse_args()
    simulate(
        args.dataset,
        limit=args.limit,
        start_row=args.start_row,
        sleep_seconds=args.sleep,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_topic=args.mqtt_topic,
        mqtt_qos=args.mqtt_qos,
    )


if __name__ == "__main__":
    main()
