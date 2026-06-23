import os
from pathlib import Path

# Pipeline mode: "simulation" reads files, "live" is reserved for real sensors.
MODE = os.getenv("ANOMX_MODE", "simulation")

BASE_DIR = Path(os.getenv("ANOMX_BASE_DIR", Path(__file__).resolve().parent))

DATA_SOURCES = {
    "simulation": {
        "FD001": os.getenv(
            "ANOMX_DATASET_PATH",
            str(BASE_DIR / "data" / "raw" / "CMAPSSData" / "train_FD001.txt"),
        ),
        "FD002": str(BASE_DIR / "data" / "raw" / "CMAPSSData" / "train_FD002.txt"),
        "FD003": str(BASE_DIR / "data" / "raw" / "CMAPSSData" / "train_FD003.txt"),
        "FD004": str(BASE_DIR / "data" / "raw" / "CMAPSSData" / "train_FD004.txt"),
    },
    "live": {
        "host": os.getenv("MQTT_HOST", "localhost"),
        "port": int(os.getenv("MQTT_PORT", "1883")),
        "topic": os.getenv("MQTT_TOPIC", "sensors/cmapss"),
    },
}

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
