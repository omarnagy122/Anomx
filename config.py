from __future__ import annotations

import os
from pathlib import Path

# Pipeline mode: "simulation" reads local C-MAPSS files; "live" is reserved for real sensors.
MODE = os.getenv("ANOMX_MODE", "simulation")

BASE_DIR = Path(os.getenv("ANOMX_BASE_DIR", Path(__file__).resolve().parent)).resolve()
DATA_DIR = Path(os.getenv("ANOMX_DATA_DIR", BASE_DIR / "data" / "raw" / "CMAPSSData")).resolve()

DATASETS = ("FD001", "FD002", "FD003", "FD004")

DATA_SOURCES = {
    "simulation": {
        dataset: str(Path(os.getenv(f"ANOMX_{dataset}_PATH", DATA_DIR / f"train_{dataset}.txt")).resolve())
        for dataset in DATASETS
    },
    "live": {
        "host": os.getenv("MQTT_HOST", "localhost"),
        "port": int(os.getenv("MQTT_PORT", "1883")),
        "topic": os.getenv("MQTT_TOPIC", "sensors/cmapss"),
    },
}

# Generic override for quick local/Airflow testing with one dataset file.
if os.getenv("ANOMX_DATASET_PATH"):
    DATA_SOURCES["simulation"][os.getenv("ANOMX_DATASET", "FD001")] = str(
        Path(os.environ["ANOMX_DATASET_PATH"]).resolve()
    )

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomx-group")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomx_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "anomx")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("POSTGRES_PASS", "anomx123"))
POSTGRES_PASS = POSTGRES_PASSWORD

SENSOR_COLUMNS = [
    "engine_id", "time_in_cycles",
    "op_setting_1", "op_setting_2", "op_setting_3",
    "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10",
    "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21",
]

USELESS_SENSOR_COLUMNS = ["s1", "s5", "s6", "s10", "s16", "s18", "s19"]
FEATURE_SENSOR_COLUMNS = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]


def dataset_path(dataset: str = "FD001") -> Path:
    dataset = dataset.upper()
    if dataset not in DATASETS:
        raise ValueError(f"Unknown dataset '{dataset}'. Expected one of: {', '.join(DATASETS)}")
    return Path(DATA_SOURCES["simulation"][dataset])
