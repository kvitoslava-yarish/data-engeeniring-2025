from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import os

DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = f"{DBT_PROJECT_DIR}/profiles"

def run_dbt_command(command: str):
    print(f"Running dbt command: {command}")
    result = subprocess.run(
        command,
        shell=True,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise Exception(f"dbt command failed: {command}")
    print("dbt command completed successfully.")


def run_dbt_silver():
    run_dbt_command(f"dbt run --select silver --profiles-dir {DBT_PROFILES_DIR}")

def run_dbt_gold():
    run_dbt_command(f"dbt run --select gold --profiles-dir {DBT_PROFILES_DIR}")

def test_dbt_gold():
    run_dbt_command(f"dbt test --select gold --profiles-dir {DBT_PROFILES_DIR}")


with DAG(
    dag_id="dbt_silver_gold_daily",
    start_date=days_ago(1),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["youtube", "dbt", "silver", "gold"],
) as dag:

    run_silver_task = PythonOperator(
        task_id="run_dbt_silver",
        python_callable=run_dbt_silver,
    )

    run_gold_task = PythonOperator(
        task_id="run_dbt_gold",
        python_callable=run_dbt_gold,
    )

    test_gold_task = PythonOperator(
        task_id="test_dbt_gold",
        python_callable=test_dbt_gold,
    )

    run_silver_task >> run_gold_task >> test_gold_task
