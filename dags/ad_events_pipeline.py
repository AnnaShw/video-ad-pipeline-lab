import json
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from src.ad_events.operators.extract_events_operator import ExtractEventsOperator
from src.ad_events.operators.transform_events_operator import TransformEventsOperator
from src.ad_events.operators.compute_metrics_operator import ComputeMetricsOperator

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

    t_extract = ExtractEventsOperator(
        task_id="extract_events",
    )

    t_transform = TransformEventsOperator(
        task_id="transform_events",
    )

    t_metrics = ComputeMetricsOperator(
        task_id="compute_metrics",
    )

    end = EmptyOperator(task_id="end")

    start >> t_extract >> t_transform >> t_metrics >> end
