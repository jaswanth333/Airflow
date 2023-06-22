from airflow.decorators import dag,task
from datetime import datetime,timedelta

default_args = {
    'owner': 'Jaswanth',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id="Taskflow_API_v2",
     default_args=default_args,
     start_date=datetime(2023,6,20),
     schedule='@daily'
     )

def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {'first_name':'Elite','last_name':'Sniper'}
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(first_name,last_name,age):
        print(f"Name is {first_name} {last_name} and age is {age}")
        
    
    name_dict=get_name()
    age=get_age()    
    greet(first_name=name_dict['first_name'],last_name=name_dict['last_name'],age=age)
    

greet_dag=hello_world_etl()