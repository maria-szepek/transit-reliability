from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import psycopg2
import os


def cleanup():

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    conn.autocommit = True
    cursor = conn.cursor()

    print("Running warehouse cleanup...")

    cleanup_queries = [

        # high-volume streaming table (keep short window)
        (
            "analytics.realtime_trip_stop_signals",
            """
            DELETE FROM analytics.realtime_trip_stop_signals
            WHERE ingestion_time < NOW() - interval '2 hours'
            """ # just because feed_timestamp is in epoch seconds its difficult
        ),

        # aggregated reliability (keep longer)
        (
            "analytics.realtime_stop_reliability",
            """
            DELETE FROM analytics.realtime_stop_reliability
            WHERE window_end < NOW() - interval '24 hours'
            """
        ),
    ]

    for table, query in cleanup_queries:
        print(f"Cleaning table: {table}")
        cursor.execute(query)
        print(f"Deleted rows from {table}: {cursor.rowcount}")

    # vacuum for performance
    print("Running VACUUM ANALYZE...")
    cursor.execute("VACUUM ANALYZE analytics.realtime_trip_stop_signals")
    cursor.execute("VACUUM ANALYZE analytics.realtime_stop_reliability")

    cursor.close()
    conn.close()

    print("Cleanup completed.")


with DAG(
    dag_id="warehouse_cleanup",
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    tags=["maintenance"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_warehouse",
        python_callable=cleanup,
    )