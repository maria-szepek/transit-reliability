# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# with DAG(
#     dag_id="transit_batch_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",
#     catchup=False,
#     tags=["transit", "gtfs"],
# ) as dag:

#     static_ingestion = BashOperator(
#         task_id="static_gtfs_ingestion",
#         bash_command="docker compose --profile batch run --rm static-gtfs-ingestion"
#     )

#     dbt_run = BashOperator(
#         task_id="dbt_run",
#         bash_command="cd dbt/analytics && uv run dbt run"
#     )

#     static_ingestion >> dbt_run