from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG definition
with DAG(
    dag_id="example_bronze_ingest_and_dbt",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example"]
) as dag:

    hello_task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow!'"
    )

