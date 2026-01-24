# Video Ad Pipeline Lab

Local Apache Airflow project for building ad-tech style data pipelines.  
Designed as a realistic lab that mirrors production ETL workflows.

---

## What This Project Shows

- Apache Airflow running locally via Docker
- Production-like DAG structure and orchestration
- Clear separation between DAGs and business logic
- Scalable Python imports across the project
- Familiar patterns used in real data engineering teams

## Tech Stack

- Apache Airflow
- Docker & Docker Compose
- Python
- Postgres (Airflow metadata)

---

---

## Pipeline Overview

**ad_events_pipeline**
- Extract mock ad events
- Transform and normalize data
- Compute basic metrics (impressions, clicks, CTR)

Task communication is handled via XCom.

---

# Current Pipeline

Name: ad_events_pipeline

- Extract mock ad events

- Transform and normalize data

- Compute basic metrics (impressions, clicks, CTR)

---

