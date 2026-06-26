import os
from pathlib import Path

# ─── Mode ────────────────────────────────────────────────
MODE = os.getenv("ANOMX_MODE", "simulation")

# ─── Paths ───────────────────────────────────────────────
BASE_DIR = Path(os.getenv("ANOMX_BASE_DIR", Path(__file__).resolve().parent)).resolve()

DATA_SOURCES = {
    "simulation": {
        "FD001": str(BASE_DIR / "data/raw/train_FD001.txt"),
        "FD002": str(BASE_DIR / "data/raw/train_FD002.txt"),
        "FD003": str(BASE_DIR / "data/raw/train_FD003.txt"),
        "FD004": str(BASE_DIR / "data/raw/train_FD004.txt"),
    },
    "live": {
        "host": os.getenv("MQTT_HOST", "localhost"),
        "port": int(os.getenv("MQTT_PORT", 1883)),
        "topic": os.getenv("MQTT_TOPIC", "sensors/cmapss"),
    }
}

# ─── Sensor Columns ──────────────────────────────────────
SENSOR_COLUMNS = [
    "engine_id", "time_in_cycles",
    "op_setting_1", "op_setting_2", "op_setting_3",
    "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10",
    "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21",
]

USELESS_SENSOR_COLUMNS = ["s1", "s5", "s6", "s10", "s16", "s18", "s19"]
FEATURE_SENSOR_COLUMNS = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]

DATASETS = ("FD001", "FD002", "FD003", "FD004")

# ─── Kafka ───────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomx-raw-consumer")

# ─── PostgreSQL ──────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomx_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "anomx")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "anomx123")

# ─── Producer ────────────────────────────────────────────
PRODUCER_SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.01"))
PRODUCER_LOG_EVERY = int(os.getenv("PRODUCER_LOG_EVERY", "1000"))

# ─── Consumer ────────────────────────────────────────────
CONSUMER_TIMEOUT_MS = int(os.getenv("CONSUMER_TIMEOUT_MS", "30000"))
CONSUMER_LOG_EVERY = int(os.getenv("CONSUMER_LOG_EVERY", "1000"))
POSTGRES_BATCH_SIZE = int(os.getenv("POSTGRES_BATCH_SIZE", "500"))
