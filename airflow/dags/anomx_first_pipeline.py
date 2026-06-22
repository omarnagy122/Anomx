
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


# ============================================================
# AnomX Phase 3 - First Working DAG
#
# Current required flow:
# start -> check_kafka -> check_postgres -> run_kafka_producer -> end
#
# Kafka is reached from inside Docker network using:
# kafka:29092
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "sensor-data"

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "anomx_db"
POSTGRES_USER = "anomx"
POSTGRES_PASSWORD = "anomx123"

DATASET_PATH = "/opt/airflow/data/raw/CMAPSSData/train_FD001.txt"
PRODUCER_SCRIPT_PATH = "/opt/airflow/ingestion/kafka_producer.py"


def check_kafka() -> None:
    """
    Check Kafka TCP connection from inside Airflow container.
    """

    host, port_text = KAFKA_BOOTSTRAP_SERVERS.split(":")
    port = int(port_text)

    print(f"Checking Kafka TCP connection: {host}:{port}")

    try:
        socket.create_connection((host, port), timeout=10).close()
    except Exception as exc:
        raise AirflowFailException(
            f"Kafka TCP check failed at {host}:{port}. Error: {exc}"
        ) from exc

    print("Kafka TCP check passed.")


def check_postgres() -> None:
    """
    Check project PostgreSQL from inside Airflow container.
    """

    print(
        "Checking PostgreSQL connection: "
        f"host={POSTGRES_HOST}, port={POSTGRES_PORT}, "
        f"database={POSTGRES_DB}, user={POSTGRES_USER}"
    )

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
        raise AirflowFailException(
            f"PostgreSQL check failed. Error: {exc}"
        ) from exc

    if result != (1,):
        raise AirflowFailException(f"Unexpected PostgreSQL result: {result}")

    print("PostgreSQL check passed.")


def run_kafka_producer() -> None:
    """
    Run the existing kafka_producer.py from inside Airflow container.
    """

    dataset_path = Path(DATASET_PATH)
    producer_script_path = Path(PRODUCER_SCRIPT_PATH)

    print(f"Checking dataset path: {dataset_path}")

    if not dataset_path.exists():
        raise AirflowFailException(f"Dataset file not found: {dataset_path}")

    if not dataset_path.is_file():
        raise AirflowFailException(f"Dataset path is not a file: {dataset_path}")

    print("Dataset file exists.")

    print(f"Checking producer script path: {producer_script_path}")

    if not producer_script_path.exists():
        raise AirflowFailException(
            f"kafka_producer.py not found: {producer_script_path}"
        )

    if not producer_script_path.is_file():
        raise AirflowFailException(
            f"Producer path is not a file: {producer_script_path}"
        )

    print("Producer script exists.")

    env = os.environ.copy()

    env["ANOMX_DATASET_PATH"] = DATASET_PATH
    env["KAFKA_BOOTSTRAP_SERVERS"] = KAFKA_BOOTSTRAP_SERVERS
    env["KAFKA_TOPIC"] = KAFKA_TOPIC
    env["PRODUCER_SLEEP_SECONDS"] = "0"
    env["PYTHONUNBUFFERED"] = "1"

    command = [sys.executable, str(producer_script_path)]

    print(f"Running producer command: {' '.join(command)}")
    print("Working directory: /opt/airflow")
    print(f"Dataset path: {DATASET_PATH}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")

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
        raise AirflowFailException(
            f"kafka_producer.py failed with exit code {return_code}"
        )

    print("kafka_producer.py finished successfully.")


default_args = {
    "owner": "anomx-phase3",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="anomx_first_kafka_pipeline",
    description="First AnomX Phase 3 DAG: check Kafka, check PostgreSQL, run kafka_producer.py",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["anomx", "phase3", "kafka"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    check_kafka_task = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka,
    )

    check_postgres_task = PythonOperator(
        task_id="check_postgres",
        python_callable=check_postgres,
    )

    run_kafka_producer_task = PythonOperator(
        task_id="run_kafka_producer",
        python_callable=run_kafka_producer,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> check_kafka_task >> check_postgres_task >> run_kafka_producer_task >> end
