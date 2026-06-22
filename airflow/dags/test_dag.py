from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
