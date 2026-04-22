from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': 300
}

with DAG('sqlmesh_dag', default_args=default_args, catchup=False) as dag:
    run_sqlmesh = BashOperator(
    task_id='run_sqlmesh',
    bash_command='cd /opt/airflow/sqlmesh && sqlmesh plan --auto-apply || sqlmesh run'
)