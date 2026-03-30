import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Directory mounted from docker compose
DATA_DIR = "/data"
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
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))


def ensure_columns(table_name, df):
    with engine.begin() as conn:
        existing = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table
            """),
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


def load_zip(zip_path: str):
    print(f"\nLoading ZIP: {zip_path}")

    inspector = inspect(engine)

    with zipfile.ZipFile(zip_path, "r") as z:
        for file in z.namelist():
            if not file.endswith(".txt"):
                continue

            table_name = os.path.basename(file).replace(".txt", "").lower()
            print(f"  -> ingesting table: {table_name}")

            with z.open(file) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False)
                df.columns = [c.lower() for c in df.columns]

                # create table if first time
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

    ensure_schema()

    for file in os.listdir(DATA_DIR):
        if file.endswith(".zip"):
            load_zip(os.path.join(DATA_DIR, file))

    print("\nIngestion completed.")


if __name__ == "__main__":
    main()