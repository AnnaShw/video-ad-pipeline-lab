import json
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from src.ad_events.tasks import compute_metrics, extract_events, transform_events 

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="as_events_pipeline",
    description="Video ad events pipeline (mock -> metrics)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["adtech", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")

    t_extract = PythonOperator(
        task_id="extract_events",
        python_callable=extract_events,
    )

    t_transform = PythonOperator(
        task_id="transform_events",
        python_callable=transform_events,
    )

    t_metrics = PythonOperator(
        task_id="compute_metrics",
        python_callable=compute_metrics,
    )

    end = EmptyOperator(task_id="end")

    start >> t_extract >> t_transform >> t_metrics >> end
