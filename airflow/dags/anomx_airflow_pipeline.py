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

DATASET_PATH = os.getenv("ANOMX_DATASET_PATH", "/opt/airflow/data/raw/CMAPSSData/train_FD001.txt")
PRODUCER_SCRIPT_PATH = "/opt/airflow/ingestion/kafka_producer.py"
CONSUMER_SCRIPT_PATH = "/opt/airflow/ingestion/kafka_consumer.py"


def check_kafka() -> None:
    host, port_text = KAFKA_BOOTSTRAP_SERVERS.split(":")
    port = int(port_text)

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
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
    except Exception as exc:
        raise AirflowFailException(f"PostgreSQL check failed. Error: {exc}") from exc

    if result != (1,):
        raise AirflowFailException(f"Unexpected PostgreSQL result: {result}")

    print("PostgreSQL check passed.")


def _run_python_script(script_path: str, extra_env: dict[str, str] | None = None) -> None:
    path = Path(script_path)
    if not path.exists():
        raise AirflowFailException(f"Script not found: {path}")

    env = os.environ.copy()
    env.update(
        {
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
    dataset_path = Path(DATASET_PATH)
    if not dataset_path.exists():
        raise AirflowFailException(f"Dataset file not found: {dataset_path}")

    _run_python_script(
        PRODUCER_SCRIPT_PATH,
        extra_env={"PRODUCER_SLEEP_SECONDS": os.getenv("PRODUCER_SLEEP_SECONDS", "0")},
    )


def run_kafka_consumer() -> None:
    _run_python_script(
        CONSUMER_SCRIPT_PATH,
        extra_env={"CONSUMER_TIMEOUT_MS": os.getenv("CONSUMER_TIMEOUT_MS", "30000")},
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
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw_sensor_data;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"raw_sensor_data row count: {count}")


default_args = {
    "owner": "anomx",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="anomx_airflow_pipeline",
    description="Improved AnomX DAG: checks Kafka/PostgreSQL, streams data, consumes it, and prevents duplicates.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["anomx", "airflow", "kafka", "postgres"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_kafka_task = PythonOperator(task_id="check_kafka", python_callable=check_kafka)
    check_postgres_task = PythonOperator(task_id="check_postgres", python_callable=check_postgres)
    run_kafka_producer_task = PythonOperator(task_id="run_kafka_producer", python_callable=run_kafka_producer)
    run_kafka_consumer_task = PythonOperator(task_id="run_kafka_consumer", python_callable=run_kafka_consumer)
    show_postgres_count_task = PythonOperator(task_id="show_postgres_count", python_callable=show_postgres_count)

    end = EmptyOperator(task_id="end")

    start >> check_kafka_task >> check_postgres_task >> run_kafka_producer_task >> run_kafka_consumer_task >> show_postgres_count_task >> end
