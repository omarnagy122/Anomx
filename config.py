import os

# ─── Pipeline Mode ───────────────────────────────────────
# "simulation" → reads from local CSV files
# "live"       → reads from real sensors via MQTT
MODE = os.getenv("ANOMX_MODE", "simulation")

# ─── Data Paths ──────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_SOURCES = {
    "simulation": {
        "FD001": os.path.join(BASE_DIR, "data/raw/train_FD001.txt"),
        "FD002": os.path.join(BASE_DIR, "data/raw/train_FD002.txt"),
        "FD003": os.path.join(BASE_DIR, "data/raw/train_FD003.txt"),
        "FD004": os.path.join(BASE_DIR, "data/raw/train_FD004.txt"),
    },
    "live": {
        "host": os.getenv("MQTT_HOST", "localhost"),
        "port": int(os.getenv("MQTT_PORT", 1883)),
        "topic": os.getenv("MQTT_TOPIC", "sensors/cmapss"),
    }
}

# ─── Kafka ───────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")

# ─── PostgreSQL ──────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB   = os.getenv("POSTGRES_DB", "anomx_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "anomx")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "anomx123")

# ─── Dataset Columns ─────────────────────────────────────
SENSOR_COLUMNS = [
    'engine_id', 'time_in_cycles',
    'op_setting_1', 'op_setting_2', 'op_setting_3',
    's1','s2','s3','s4','s5','s6','s7','s8','s9','s10',
    's11','s12','s13','s14','s15','s16','s17','s18','s19','s20','s21'
]
