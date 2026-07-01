from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = Path(os.getenv("MODEL_PATH", PROJECT_ROOT / "model" / "xgboost_predictive_model.pkl")).resolve()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomx_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "anomx")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("POSTGRES_PASS", "anomx123"))

DB_URL = (
    "postgresql+psycopg2://"
    f"{quote_plus(POSTGRES_USER)}:{quote_plus(POSTGRES_PASSWORD)}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

MODEL_FEATURES = [
    "time_in_cycles",
    "op_setting_1", "op_setting_2", "op_setting_3",
    "s2", "s3", "s4", "s7", "s8", "s9", "s11", "s12", "s13", "s14", "s15", "s17", "s20", "s21",
    "rolling_avg_s2", "rolling_std_s2", "delta_s2",
    "rolling_avg_s3", "rolling_std_s3", "delta_s3",
    "rolling_avg_s4", "rolling_std_s4", "delta_s4",
    "rolling_avg_s7", "rolling_std_s7", "delta_s7",
    "rolling_avg_s11", "rolling_std_s11", "delta_s11",
    "rolling_avg_s12", "rolling_std_s12", "delta_s12",
    "rolling_avg_s15", "rolling_std_s15", "delta_s15",
    "rolling_avg_s20", "rolling_std_s20", "delta_s20",
    "rolling_avg_s21", "rolling_std_s21", "delta_s21",
]
