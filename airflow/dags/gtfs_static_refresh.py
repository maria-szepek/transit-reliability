# Airflow DAG that refreshes GTFS static data for either the local Postgres
# warehouse or the hybrid GCS/BigQuery warehouse path.

from __future__ import annotations

import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path("/opt/project")
DATA_DIR = PROJECT_ROOT / "data"

TMP_DIR = DATA_DIR / "tmp"
ARCHIVE_DIR = DATA_DIR / "archive"
WAREHOUSE_BACKEND = os.getenv("WAREHOUSE_BACKEND", "postgres").lower()
GTFS_LOADER_SCRIPT = (
    "services/ingestion/gtfs/load_gtfs_bigquery.py"
    if WAREHOUSE_BACKEND == "bigquery"
    else "services/ingestion/gtfs/load_gtfs.py"
)

GTFS_FEEDS = {
    "gtfs_b.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_b.zip",
    "gtfs_busco.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_busco.zip",
    "gtfs_bx.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_bx.zip",
    "gtfs_m.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_m.zip",
    "gtfs_q.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_q.zip",
    "gtfs_si.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_si.zip",
    "gtfs_subway.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip",
    "gtfslirr.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfslirr.zip",
    "gtfsmnr.zip": "https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip",
}


default_args = {
    "owner": "maria",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def ensure_dirs():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def download_all_gtfs():
    ensure_dirs()

    for filename, url in GTFS_FEEDS.items():
        target = DATA_DIR / filename
        tmp = TMP_DIR / filename

        print(f"Downloading {filename} ...")

        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        with open(tmp, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        # archive previous version
        if target.exists():
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            archived = ARCHIVE_DIR / f"{filename}.{ts}"
            shutil.move(target, archived)

        shutil.move(tmp, target)


with DAG(
    dag_id="gtfs_static_refresh",
    start_date=datetime(2026, 4, 1),
    schedule="@weekly",
    catchup=False,
    default_args=default_args,
    tags=["gtfs"],
) as dag:

    download = PythonOperator(
        task_id="download_gtfs_feeds",
        python_callable=download_all_gtfs,
    )

    ingest = BashOperator(
        task_id="ingest_gtfs",
        bash_command=(
            "cd /opt/project && "
            f"python -u {GTFS_LOADER_SCRIPT}"
        ),
    )

    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command=(
            "cd /opt/project/dbt/analytics && "
            "dbt run --profiles-dir /opt/project/dbt"
        ),
    )

    rebuild_otp = BashOperator(
        task_id="rebuild_otp",
        bash_command="docker restart transit-reliability-otp-1",
        pool="otp_rebuild_pool",
    )

    download >> ingest >> dbt_run
    download >> rebuild_otp

