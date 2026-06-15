# Airflow DAG that refreshes the local OSM extract used by OpenTripPlanner.

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import requests
import shutil

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = Path("/opt/project")
DATA_DIR = PROJECT_ROOT / "data"

OSM_URL = "https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf"
OSM_FILE = DATA_DIR / "new-york-latest.osm.pbf"


def download_osm():
    print("Downloading latest OSM...")

    response = requests.get(OSM_URL, stream=True, timeout=300)
    response.raise_for_status()

    tmp = OSM_FILE.with_suffix(".tmp")

    with open(tmp, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    shutil.move(tmp, OSM_FILE)

    print("OSM download complete.")


with DAG(
    dag_id="osm_refresh",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["osm"],
) as dag:

    download = PythonOperator(
        task_id="download_osm",
        python_callable=download_osm,
    )

    rebuild_otp = BashOperator(
        task_id="rebuild_otp",
        bash_command="docker restart transit-reliability-otp-1",
        pool="otp_rebuild_pool",
    )

    download >> rebuild_otp
