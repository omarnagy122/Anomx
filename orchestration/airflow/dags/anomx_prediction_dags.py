from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "anomx",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="anomx_trained_model_prediction_pipeline",
    description="AnomX scheduled trained-model prediction from processed_sensor_data",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["anomx", "prediction", "trained-model", "schedule-settings"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_prediction = BashOperator(
        task_id="run_trained_model_prediction_from_processed_data",
        bash_command=(
            "cd /opt/airflow && "
            "python /opt/airflow/src/prediction/prediction_pipeline.py "
            "--run-type airflow_scheduled_trained_model "
            "--use-trained-model --respect-schedule"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> run_prediction >> end
