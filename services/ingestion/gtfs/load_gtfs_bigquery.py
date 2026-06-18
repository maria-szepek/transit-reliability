# Uploads downloaded GTFS static zip files to GCS and derives BigQuery raw
# tables from those local archives.

from datetime import datetime, timezone
import os
import zipfile

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage


PROJECT_ID_FROM_ENV = os.getenv("GCP_PROJECT_ID")
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "infra/keys/credentials.json")

RAW_DATASET = "raw"
LOCATION = "US"
GCS_PREFIX = "gtfs/static"
DATA_DIR = os.getenv("DATA_DIR", "/opt/project/data")
GTFS_RUN_ID = os.getenv(
    "GTFS_RUN_ID",
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
)


def require_env(name: str, value: str | None) -> str:
    if not value:
        raise ValueError(f"{name} must be set")
    return value


PROJECT_ID = require_env("GCP_PROJECT_ID", PROJECT_ID_FROM_ENV)
BUCKET_NAME = f"{PROJECT_ID}-transit-reliability-raw"

bigquery_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)
loaded_tables: set[str] = set()


def dataset_ref():
    return bigquery.DatasetReference(PROJECT_ID, RAW_DATASET)


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"


def gcs_object_name(filename: str) -> str:
    return f"{GCS_PREFIX}/run_id={GTFS_RUN_ID}/{filename}"


def upload_zip(zip_path: str, filename: str):
    object_name = gcs_object_name(filename)

    print(f"Uploading {filename} to gs://{BUCKET_NAME}/{object_name}", flush=True)

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    blob.chunk_size = 8 * 1024 * 1024
    blob.upload_from_filename(zip_path, timeout=600)


def ensure_dataset():
    dataset = bigquery.Dataset(dataset_ref())
    dataset.location = LOCATION

    try:
        bigquery_client.get_dataset(dataset.reference)
    except NotFound:
        print(f"Creating BigQuery dataset {RAW_DATASET}", flush=True)
        bigquery_client.create_dataset(dataset)


def delete_existing_tables():
    print("Deleting existing GTFS raw tables", flush=True)

    for table in bigquery_client.list_tables(dataset_ref()):
        print(f"  deleting {table.table_id}", flush=True)
        bigquery_client.delete_table(table.reference)


def schema_for_columns(columns):
    return [bigquery.SchemaField(column, "STRING") for column in columns]


def ensure_table(table_name: str, columns):
    target_table_id = table_id(table_name)

    try:
        table = bigquery_client.get_table(target_table_id)
    except NotFound:
        print(f"    creating table {target_table_id}", flush=True)
        table = bigquery.Table(target_table_id, schema=schema_for_columns(columns))
        return bigquery_client.create_table(table)

    existing_fields = {field.name for field in table.schema}
    missing_fields = [
        bigquery.SchemaField(column, "STRING")
        for column in columns
        if column not in existing_fields
    ]

    if missing_fields:
        for field in missing_fields:
            print(f"    adding column {field.name}", flush=True)

        table.schema = [*table.schema, *missing_fields]
        table = bigquery_client.update_table(table, ["schema"])

    return table


def load_dataframe(table_name: str, df: pd.DataFrame):
    table = ensure_table(table_name, df.columns)
    table_columns = [field.name for field in table.schema]

    for column in table_columns:
        if column not in df.columns:
            df[column] = None

    df = df[table_columns]

    write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND
        if table_name in loaded_tables
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        write_disposition=write_disposition,
    )

    load_job = bigquery_client.load_table_from_dataframe(
        df,
        table.full_table_id.replace(":", "."),
        job_config=job_config,
    )
    load_job.result()
    loaded_tables.add(table_name)

    print(f"    loaded {len(df)} rows", flush=True)


def load_zip(zip_path: str, filename: str):
    print(f"\nLoading ZIP into BigQuery: {zip_path}", flush=True)

    upload_zip(zip_path, filename)

    with zipfile.ZipFile(zip_path, "r") as z:
        for file in z.namelist():
            if not file.endswith(".txt"):
                continue

            table_name = os.path.basename(file).replace(".txt", "").lower()
            print(f"  -> ingesting table: {table_name}", flush=True)

            with z.open(file) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False)
                df.columns = [column.lower() for column in df.columns]

                load_dataframe(table_name, df)


def main():
    print("Starting GTFS BigQuery ingestion...", flush=True)
    print(f"Using project: {PROJECT_ID}", flush=True)
    print(f"Using bucket: {BUCKET_NAME}", flush=True)
    print(f"Using raw dataset: {RAW_DATASET}", flush=True)
    print(f"Using GCS prefix: {GCS_PREFIX}", flush=True)
    print(f"Using data dir: {DATA_DIR}", flush=True)
    print(f"Using GTFS run id: {GTFS_RUN_ID}", flush=True)

    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(f"DATA_DIR does not exist: {DATA_DIR}")

    ensure_dataset()
    delete_existing_tables()

    zip_files = sorted(
        file for file in os.listdir(DATA_DIR) if file.endswith(".zip")
    )

    if not zip_files:
        raise FileNotFoundError(f"No .zip files found in {DATA_DIR}")

    for filename in zip_files:
        load_zip(os.path.join(DATA_DIR, filename), filename)

    print("\nBigQuery ingestion completed.", flush=True)


if __name__ == "__main__":
    main()
