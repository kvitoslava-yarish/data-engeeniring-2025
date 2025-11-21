from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from docker.types import Mount
import os

DBT_IMAGE = "local/dbt-clickhouse:latest"
DBT_PROJECT_DIR = "/usr/app"
DBT_PROFILES_DIR = f"{DBT_PROJECT_DIR}/profiles"
HOST_DBT_PATH = "/home/kyarish/Tartu/DE/data-engeeniring-2025/project2/dbt"

with DAG(
    dag_id="dbt_silver_gold_daily",
    start_date=days_ago(1),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["dbt", "clickhouse", "silver", "gold"],
) as dag:

    run_silver = DockerOperator(
        task_id="dbt_run_silver",
        image=DBT_IMAGE,
        command=f"dbt run --select silver --profiles-dir {DBT_PROFILES_DIR}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source=HOST_DBT_PATH, target=DBT_PROJECT_DIR, type="bind")
        ],
        environment={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_HOST": "clickhouse",
            "DBT_USER": "default",
            "DBT_PASSWORD": "password",
            "DBT_SCHEMA": "analytics",
        },
        auto_remove=True,
    )

    run_gold = DockerOperator(
        task_id="dbt_run_gold",
        image=DBT_IMAGE,
        command=f"dbt run --select gold --profiles-dir {DBT_PROFILES_DIR}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source=HOST_DBT_PATH, target=DBT_PROJECT_DIR, type="bind")
        ],
        auto_remove=True,
    )

    test_gold = DockerOperator(
        task_id="dbt_test_gold",
        image=DBT_IMAGE,
        command=f"dbt test --select gold --profiles-dir {DBT_PROFILES_DIR}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source=HOST_DBT_PATH, target=DBT_PROJECT_DIR, type="bind")
        ],
        auto_remove=True,
    )

    run_silver >> run_gold >> test_gold

