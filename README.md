# Video Ad Pipeline Lab

Local Apache Airflow project for building ad-tech style data pipelines.  
Designed as a realistic lab that mirrors production ETL workflows.

---

## What This Project Shows

- Apache Airflow running locally via Docker
- Custom Operator Design (`ExtractEventsOperator`, `TransformEventsOperator`, `ComputeMetricsOperator`)
- Public Data Extraction (keyless YouTube ad metric scraping simulating live performance)
- In-Memory SQL Compute (calculating downstream analytical metrics using embedded `sqlite3` and pure `.sql` scripts)
- Clear separation between DAGs, Operators, and core business logic
- Powerful native Python scraping without relying on static mocks

## Tech Stack

- Apache Airflow
- Docker & Docker Compose
- Python & `requests`
- SQLite (for local metric computation)
- Postgres (Airflow Metadata)

---

## Pipeline Overview

**ad_events_pipeline**
- **Extract (`ExtractEventsOperator`)**: Scrapes live YouTube watch pages for popular video commercials, converting raw total views and likes directly into thousands of realistically timestamped local JSON events (impressions and clicks).
- **Transform (`TransformEventsOperator`)**: Validates and normalizes the raw XCom JSON payloads.
- **Compute (`ComputeMetricsOperator`)**: Ingests JSON into a dynamic embedded SQLite table and executes the external `sql/marts.sql` query to calculate real-world analytical performance metrics (like grouped clicks, impressions, and CTR) separated perfectly by `campaign_id`.

Task communication is handled natively via injected XCom streams flowing smoothly between the custom operators
