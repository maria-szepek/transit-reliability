# Loads downloaded GTFS static zip files into the raw Postgres schema.

import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Directory containing GTFS zip files
DATA_DIR = os.getenv("DATA_DIR", "/opt/project/data")
SCHEMA = "raw"

# Read DB config from environment
POSTGRES_USER = os.getenv("POSTGRES_USER", "transit")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "transit")
POSTGRES_DB = os.getenv("POSTGRES_DB", "transit")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URI = (
    f"postgresql://{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)

engine = create_engine(DB_URI)


def ensure_schema():
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))


def ensure_columns(table_name, df):
    with engine.begin() as conn:
        existing = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                  AND table_name = :table
                """
            ),
            {"schema": SCHEMA, "table": table_name},
        )

        existing_cols = {r[0] for r in existing}

        for col in df.columns:
            if col not in existing_cols:
                print(f"    adding column {col}")
                conn.execute(
                    text(
                        f'ALTER TABLE "{SCHEMA}"."{table_name}" '
                        f'ADD COLUMN "{col}" TEXT'
                    )
                )


def truncate_all_tables():
    print("Truncating existing GTFS tables")

    inspector = inspect(engine)

    with engine.begin() as conn:
        for table in inspector.get_table_names(schema=SCHEMA):
            print(f"  truncating {table}")
            conn.execute(
                text(f'TRUNCATE TABLE "{SCHEMA}"."{table}"')
            )


def load_zip(zip_path: str):
    print(f"\nLoading ZIP: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as z:
        inspector = inspect(engine)

        for file in z.namelist():
            if not file.endswith(".txt"):
                continue

            table_name = os.path.basename(file).replace(".txt", "").lower()
            print(f"  -> ingesting table: {table_name}")

            with z.open(file) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False)
                df.columns = [c.lower() for c in df.columns]

                if not inspector.has_table(table_name, schema=SCHEMA):
                    print(f"    creating table {SCHEMA}.{table_name}")
                    df.head(0).to_sql(
                        table_name,
                        engine,
                        schema=SCHEMA,
                        index=False,
                    )

                ensure_columns(table_name, df)

                df.to_sql(
                    table_name,
                    engine,
                    schema=SCHEMA,
                    if_exists="append",
                    index=False,
                    chunksize=5000,
                    method="multi",
                )


def main():
    print("Starting GTFS ingestion...")
    print(f"Using DB: {DB_URI}")
    print(f"Using data dir: {DATA_DIR}")

    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(f"DATA_DIR does not exist: {DATA_DIR}")

    ensure_schema()
    truncate_all_tables()

    zip_files = sorted(
        file for file in os.listdir(DATA_DIR) if file.endswith(".zip")
    )

    if not zip_files:
        raise FileNotFoundError(f"No .zip files found in {DATA_DIR}")

    for file in zip_files:
        load_zip(os.path.join(DATA_DIR, file))

    print("\nIngestion completed.")


if __name__ == "__main__":
    main()
