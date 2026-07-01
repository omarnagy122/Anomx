from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from prediction.trained_model import run_trained_model_prediction  # noqa: E402


def run_pipeline_from_airflow() -> dict:
    return run_trained_model_prediction(run_type="legacy_airflow_dag_trained_model", respect_schedule=True)


default_args = {
    "owner": "AnomX_Team",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "anomx_batch_inference_pipeline",
    default_args=default_args,
    description="Compatibility DAG for trained XGBoost inference using schedule_settings",
    schedule_interval="@hourly",
    catchup=False,
)

run_inference_task = PythonOperator(
    task_id="execute_trained_model_inference",
    python_callable=run_pipeline_from_airflow,
    dag=dag,
)
