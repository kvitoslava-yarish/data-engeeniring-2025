from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_silver_layer',
    default_args=default_args,
    description='Run dbt silver layer transformations',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'silver']
) as dag:
    
    run_silver = BashOperator(
        task_id='run_silver_models',
        bash_command='cd /opt/dbt && dbt run --models silver --profiles-dir /opt/dbt/profiles',
    )
    
    test_silver = BashOperator(
        task_id='test_silver_models',
        bash_command='cd /opt/dbt && dbt test --models silver --profiles-dir /opt/dbt/profiles',
    )
    
    run_silver >> test_silver