from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def hello():
    print("Hello from Transit Reliability Pipeline 🚀")


with DAG(
    dag_id="hello_transit_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["transit", "test"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )

    hello_task