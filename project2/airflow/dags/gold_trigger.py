from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_gold_layer',
    default_args=default_args,
    description='Run dbt gold layer transformations',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'gold']
) as dag:
    
    run_gold = BashOperator(
        task_id='run_gold_models',
        bash_command='cd /opt/dbt && dbt run --models gold --profiles-dir /opt/dbt/profiles',
    )
    
    test_gold = BashOperator(
        task_id='test_gold_models',
        bash_command='cd /opt/dbt && dbt test --models gold --profiles-dir /opt/dbt/profiles',
    )
    
    run_gold >> test_gold