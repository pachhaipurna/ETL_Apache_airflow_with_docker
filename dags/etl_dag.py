"""
Airflow DAG: simple ETL pipeline (extract -> transform -> load).

Wires the same extract/transform/load logic into an Airflow DAG using
PythonOperator tasks, so it can run on a schedule inside the Airflow
container defined in docker-compose.yml.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

SOURCE_PATH = Path("data/raw/input.csv")
OUTPUT_PATH = Path("data/processed/output.csv")


def extract(**context) -> None:
    """Read raw records from a CSV source and push them to XCom.

    Replace this with a real source: a database query, an API call,
    a cloud storage read, etc.
    """
    if not SOURCE_PATH.exists():
        logger.warning("Source file %s not found, returning empty dataset", SOURCE_PATH)
        rows: list[dict[str, Any]] = []
    else:
        with SOURCE_PATH.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    logger.info("Extracted %d rows from %s", len(rows), SOURCE_PATH)
    context["ti"].xcom_push(key="raw_rows", value=rows)


def transform(**context) -> None:
    """Clean and reshape rows pulled from the extract task via XCom.

    Replace this with your real transformation logic: type coercion,
    filtering, deduplication, joins, derived columns, etc.
    """
    rows = context["ti"].xcom_pull(key="raw_rows", task_ids="extract") or []

    cleaned = []
    for row in rows:
        cleaned_row = {k: v.strip() if isinstance(v, str) else v for k, v in row.items()}
        if any(cleaned_row.values()):
            cleaned.append(cleaned_row)

    logger.info("Transformed %d rows into %d valid rows", len(rows), len(cleaned))
    context["ti"].xcom_push(key="clean_rows", value=cleaned)


def load(**context) -> None:
    """Write transformed rows pulled from the transform task to the destination.

    Replace this with a real sink: a database table, a warehouse,
    cloud storage, etc.
    """
    rows = context["ti"].xcom_pull(key="clean_rows", task_ids="transform") or []

    if not rows:
        logger.warning("No rows to load, skipping write")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Loaded %d rows into %s", len(rows), OUTPUT_PATH)


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="etl_pipeline",
    description="Simple extract -> transform -> load pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "example"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
