import json
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ad_events_pipeline",
    description="Ad events ETL pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["adtech", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
