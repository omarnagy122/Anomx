from __future__ import annotations

import os
import socket
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomx-airflow-group")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomx_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "anomx")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "anomx123")

ANOMX_DATASET = os.getenv("ANOMX_DATASET", "FD001")
DATASET_PATH = os.getenv("ANOMX_DATASET_PATH", f"/opt/airflow/data/raw/CMAPSSData/train_{ANOMX_DATASET}.txt")
PRODUCER_SCRIPT_PATH = "/opt/airflow/ingestion/kafka_producer.py"
CONSUMER_SCRIPT_PATH = "/opt/airflow/ingestion/kafka_consumer.py"


def _split_host_port(address: str) -> tuple[str, int]:
    if ":" not in address:
        raise AirflowFailException(f"Invalid host:port address: {address}")
    host, port_text = address.rsplit(":", 1)
    return host, int(port_text)


def check_kafka() -> None:
    host, port = _split_host_port(KAFKA_BOOTSTRAP_SERVERS)
    print(f"Checking Kafka TCP connection: {host}:{port}")
    try:
        socket.create_connection((host, port), timeout=10).close()
    except Exception as exc:
        raise AirflowFailException(f"Kafka TCP check failed at {host}:{port}. Error: {exc}") from exc
    print("Kafka TCP check passed.")


def check_postgres() -> None:
    print(f"Checking PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10,
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
        conn.close()
    except Exception as exc:
        raise AirflowFailException(f"PostgreSQL check failed. Error: {exc}") from exc

    if result != (1,):
        raise AirflowFailException(f"Unexpected PostgreSQL result: {result}")
    print("PostgreSQL check passed.")


def check_dataset() -> None:
    dataset_path = Path(DATASET_PATH)
    print(f"Checking dataset path: {dataset_path}")
    if not dataset_path.exists() or not dataset_path.is_file():
        raise AirflowFailException(
            f"Dataset file not found: {dataset_path}. "
            "Mount or copy C-MAPSS data to data/raw/CMAPSSData before triggering the DAG."
        )
    print("Dataset file exists.")


def _run_python_script(script_path: str, extra_env: dict[str, str] | None = None) -> None:
    path = Path(script_path)
    if not path.exists() or not path.is_file():
        raise AirflowFailException(f"Script not found: {path}")

    env = os.environ.copy()
    env.update(
        {
            "ANOMX_DATASET": ANOMX_DATASET,
            "ANOMX_DATASET_PATH": DATASET_PATH,
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": KAFKA_TOPIC,
            "KAFKA_GROUP_ID": KAFKA_GROUP_ID,
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_PORT": str(POSTGRES_PORT),
            "POSTGRES_DB": POSTGRES_DB,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "PYTHONUNBUFFERED": "1",
        }
    )
    if extra_env:
        env.update(extra_env)

    command = [sys.executable, str(path)]
    print(f"Running command: {' '.join(command)}")

    process = subprocess.Popen(
        command,
        cwd="/opt/airflow",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="")

    return_code = process.wait()
    if return_code != 0:
        raise AirflowFailException(f"{path.name} failed with exit code {return_code}")


def run_kafka_producer() -> None:
    _run_python_script(
        PRODUCER_SCRIPT_PATH,
        extra_env={
            "PRODUCER_SLEEP_SECONDS": os.getenv("PRODUCER_SLEEP_SECONDS", "0"),
            "PRODUCER_LOG_EVERY": os.getenv("PRODUCER_LOG_EVERY", "1000"),
        },
    )


def run_kafka_consumer() -> None:
    _run_python_script(
        CONSUMER_SCRIPT_PATH,
        extra_env={
            "CONSUMER_TIMEOUT_MS": os.getenv("CONSUMER_TIMEOUT_MS", "30000"),
            "POSTGRES_BATCH_SIZE": os.getenv("POSTGRES_BATCH_SIZE", "500"),
            "CONSUMER_LOG_EVERY": os.getenv("CONSUMER_LOG_EVERY", "1000"),
        },
    )


def show_postgres_count() -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM raw_sensor_data;")
        count = cursor.fetchone()[0]
    conn.close()
    print(f"raw_sensor_data row count: {count}")


default_args = {
    "owner": "anomx",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="anomx_airflow_pipeline",
    description="AnomX DAG: validate services, stream C-MAPSS data through Kafka, sink raw records to PostgreSQL idempotently.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["anomx", "airflow", "kafka", "postgres", "cmapss"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_kafka_task = PythonOperator(task_id="check_kafka", python_callable=check_kafka)
    check_postgres_task = PythonOperator(task_id="check_postgres", python_callable=check_postgres)
    check_dataset_task = PythonOperator(task_id="check_dataset", python_callable=check_dataset)
    run_kafka_producer_task = PythonOperator(task_id="run_kafka_producer", python_callable=run_kafka_producer)
    run_kafka_consumer_task = PythonOperator(task_id="run_kafka_consumer", python_callable=run_kafka_consumer)
    show_postgres_count_task = PythonOperator(task_id="show_postgres_count", python_callable=show_postgres_count)

    end = EmptyOperator(task_id="end")

    start >> check_kafka_task >> check_postgres_task >> check_dataset_task >> run_kafka_producer_task >> run_kafka_consumer_task >> show_postgres_count_task >> end
