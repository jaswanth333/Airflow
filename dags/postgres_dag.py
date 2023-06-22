from airflow import DAG
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag_owner = ''

default_args = {'owner': 'jaswanth',
        'retries': 2,
        'retry_delay': timedelta(minutes=0)
        }

with DAG(
    dag_id='post_gres_connection_and_insertion_dag',
    default_args=default_args,
    start_date=datetime(2023,6,21),
    schedule='0 0 * * *'
) as dag:
    # from airflow.providers.postgres.operators.postgres import PostgresOperator
        create_table = PostgresOperator(
        task_id='create_postgres',
        postgres_conn_id='postgres_localhost',
        sql="""
        create table if not exists dag_runs
        (
         dt date,
         dag_id character varying,
         primary key(dt,dag_id)   
        )
        """
        )
        create_table 
        
    # from airflow.providers.postgres.operators.postgres import PostgresOperator
        insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres_localhost',
        sql="""
         insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
        )

                
        delete_data = PostgresOperator(
        task_id='delete_data',
        postgres_conn_id='postgres_localhost',
        sql="""
         delete from dag_runs where dt= ('{{ ds }}' and dag_id= '{{ dag.dag_id }}')
        """
        )
create_table  >> delete_data >> insert_data        
        