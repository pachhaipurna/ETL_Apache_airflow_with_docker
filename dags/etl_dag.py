"""
Standalone ETL script (plain Python, no Airflow dependencies).

Structure:
    extract() -> read raw data from a source
    transform() -> clean / reshape the data
    load() -> write the result to a destination

Run directly with: python dags/etl_dag.py
"""

import csv
import logging
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SOURCE_PATH = Path("data/raw/input.csv")
OUTPUT_PATH = Path("data/processed/output.csv")


def extract(source_path: Path = SOURCE_PATH) -> list[dict[str, Any]]:
    """Read raw records from a CSV source.

    Replace this with a real source: a database query, an API call,
    a cloud storage read, etc.
    """
    if not source_path.exists():
        logger.warning("Source file %s not found, returning empty dataset", source_path)
        return []

    with source_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    logger.info("Extracted %d rows from %s", len(rows), source_path)
    return rows


def transform(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Clean and reshape extracted rows.

    Replace this with your real transformation logic: type coercion,
    filtering, deduplication, joins, derived columns, etc.
    """
    cleaned = []
    for row in rows:
        cleaned_row = {k: v.strip() if isinstance(v, str) else v for k, v in row.items()}
        if any(cleaned_row.values()):
            cleaned.append(cleaned_row)

    logger.info("Transformed %d rows into %d valid rows", len(rows), len(cleaned))
    return cleaned


def load(rows: list[dict[str, Any]], output_path: Path = OUTPUT_PATH) -> None:
    """Write transformed rows to the destination.

    Replace this with a real sink: a database table, a warehouse,
    cloud storage, etc.
    """
    if not rows:
        logger.warning("No rows to load, skipping write")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Loaded %d rows into %s", len(rows), output_path)


def run_etl() -> None:
    raw = extract()
    clean = transform(raw)
    load(clean)


if __name__ == "__main__":
    run_etl()
