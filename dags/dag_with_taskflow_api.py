from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

@dag(dag_id='A_dag_with_taskflow_api_v01',
        default_args=default_args,
        start_date=datetime(2025, 3, 6, 15), # Year, Month, Day, Hour, Minute, Second
        schedule_interval='@daily')
def hello_world_etl():
    @task()
    def get_name():
        return 'Christophe'
    @task()
    def get_role():
        return 'Lecturer'
    @task()
    def greet(name, role):
        print(f'Hello from the greet function '
            f'Hi, my name is {name}, my role is {role}!')

    name = get_name()
    role = get_role()
    greet(name=name, role=role)

greet_dag = hello_world_etl() 