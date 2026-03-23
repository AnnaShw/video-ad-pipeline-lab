import json
import sqlite3
from typing import Dict, List
from airflow.models import BaseOperator

class ComputeMetricsOperator(BaseOperator):
    """
    Custom Operator to compute ad metrics using pure SQL on an in-memory SQLite database.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context) -> None:
        raw = context["ti"].xcom_pull(task_ids="transform_events", key="clean_events")
        events: List[Dict] = json.loads(raw) if raw else []
        
        # We will use Python's built-in sqlite3 to run SQL locally on the mock data
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()
        
        # Create a temporary table structure matching our events
        cursor.execute('''
            CREATE TABLE events (
                ts TEXT,
                event TEXT,
                campaign_id TEXT,
                country TEXT
            )
        ''')
        
        # Load the JSON records into the SQL table
        for e in events:
            cursor.execute(
                "INSERT INTO events (ts, event, campaign_id, country) VALUES (?, ?, ?, ?)",
                (e.get("ts"), e.get("event"), str(e.get("campaign_id")), e.get("country"))
            )
            
        # Read the required SQL Metric Calculation from the separate sql directory
        from pathlib import Path
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        sql_path = project_root / 'sql' / 'marts.sql'
        
        with open(sql_path, 'r') as f:
            sql_query = f.read()

        cursor.execute(sql_query)
        rows = cursor.fetchall()
        
        metrics = []
        for row in rows:
            metrics.append({
                "campaign_id": row[0],
                "impressions": row[1] or 0,
                "clicks": row[2] or 0,
                "ctr": row[3] or 0.0,
            })
        
        self.log.info(f"Calculated Metrics via SQL: {metrics}")
        
        context["ti"].xcom_push(key="metrics", value=json.dumps(metrics))
        conn.close()
