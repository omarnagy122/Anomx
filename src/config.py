from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = Path(os.getenv("ANOMX_BASE_DIR", PROJECT_ROOT)).resolve()
DATA_DIR = Path(os.getenv("ANOMX_DATA_DIR", BASE_DIR / "data" / "raw" / "CMAPSSData")).resolve()

SENSOR_COLUMNS = [
    "engine_id", "time_in_cycles",
    "op_setting_1", "op_setting_2", "op_setting_3",
    "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10",
    "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21",
]

DATASETS = ("FD001", "FD002", "FD003", "FD004")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomx-raw-consumer")

# MQTT broker used for optional machine/IoT simulation before Kafka.
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/cmapss")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "anomx-client")
MQTT_QOS = int(os.getenv("MQTT_QOS", "0"))
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomx_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "anomx")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("POSTGRES_PASS", "anomx123"))

PRODUCER_SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.01"))
PRODUCER_LOG_EVERY = int(os.getenv("PRODUCER_LOG_EVERY", "1000"))
CONSUMER_LOG_EVERY = int(os.getenv("CONSUMER_LOG_EVERY", "1000"))
CONSUMER_TIMEOUT_MS = int(os.getenv("CONSUMER_TIMEOUT_MS", "0"))  # 0 = run continuously
POSTGRES_COMMIT_EVERY = int(os.getenv("POSTGRES_COMMIT_EVERY", "1"))  # default row-by-row for real-time

PREDICTION_WINDOW_CYCLES = int(os.getenv("PREDICTION_WINDOW_CYCLES", "50"))
ALERT_HIGH_THRESHOLD = float(os.getenv("ALERT_HIGH_THRESHOLD", "70"))
ALERT_CRITICAL_THRESHOLD = float(os.getenv("ALERT_CRITICAL_THRESHOLD", "90"))

USELESS_SENSOR_COLUMNS = ["s1", "s5", "s6", "s10", "s16", "s18", "s19"]
FEATURE_SENSOR_COLUMNS = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]


def dataset_path(dataset: str = "FD001") -> Path:
    dataset = dataset.upper()
    override = os.getenv(f"ANOMX_{dataset}_PATH")
    if override:
        return Path(override).resolve()
    generic_override = os.getenv("ANOMX_DATASET_PATH")
    if generic_override:
        return Path(generic_override).resolve()
    return (DATA_DIR / f"train_{dataset}.txt").resolve()
