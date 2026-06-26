from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append("/media/data/omar/programming course/DEPI/DEPI project/anomx")

from ingestion.kafka_producer import stream_simulation
from processing.spark_processor import process_dataset

default_args = {
    'owner': 'omar',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='anomx_pipeline',
    default_args=default_args,
    description='AnomX End-to-End Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['anomx', 'predictive-maintenance'],
) as dag:

    for dataset in ['FD001', 'FD002', 'FD003', 'FD004']:

        ingest = PythonOperator(
            task_id=f'ingest_{dataset}',
            python_callable=stream_simulation,
            op_kwargs={'dataset': dataset},
        )

        process = PythonOperator(
            task_id=f'process_{dataset}',
            python_callable=process_dataset,
            op_kwargs={'dataset': dataset},
        )

        ingest >> process
