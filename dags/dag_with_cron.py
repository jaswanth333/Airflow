from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Jaswanth',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_cron_expression_v05",
    start_date=datetime(2023, 5, 1),
    schedule_interval='0 4 1-15 Mar-Jul Thu,Fri'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo dag with cron expression!"
    )
    task1
    
  