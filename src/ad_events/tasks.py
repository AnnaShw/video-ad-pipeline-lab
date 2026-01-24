import json
from typing import Dict, List


def extract_events(**context) -> None:
    """Extract raw ad events (mock source)."""
    events: List[Dict] = [
        {"ts": "2026-01-24T10:00:00Z", "event": "impression", "campaign_id": 101, "country": "IL"},
        {"ts": "2026-01-24T10:00:01Z", "event": "click", "campaign_id": 101, "country": "IL"},
        {"ts": "2026-01-24T10:00:02Z", "event": "impression", "campaign_id": 202, "country": "US"},
    ]
    context["ti"].xcom_push(key="raw_events", value=json.dumps(events))


def transform_events(**context) -> None:
    """Validate and normalize events."""
    raw = context["ti"].xcom_pull(task_ids="extract_events", key="raw_events")
    events: List[Dict] = json.loads(raw) if raw else []
    # Placeholder for schema validation / enrichment
    context["ti"].xcom_push(key="clean_events", value=json.dumps(events))


def compute_metrics(**context) -> None:
    """Compute basic ad metrics."""
    raw = context["ti"].xcom_pull(task_ids="transform_events", key="clean_events")
    events: List[Dict] = json.loads(raw) if raw else []

    impressions = sum(1 for e in events if e.get("event") == "impression")
    clicks = sum(1 for e in events if e.get("event") == "click")
    ctr = (clicks / impressions) if impressions else 0.0

    metrics = {
        "impressions": impressions,
        "clicks": clicks,
        "ctr": ctr,
    }
    context["ti"].xcom_push(key="metrics", value=json.dumps(metrics))
