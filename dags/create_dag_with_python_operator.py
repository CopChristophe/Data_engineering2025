from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}


def greet(ti, role):
    firstname=ti.xcom_pull(task_ids='get_name',key='firstname')
    lastname=ti.xcom_pull(task_ids='get_name',key='lastname')
    name=f'{firstname} {lastname}'
    print(f'Hello from the greet function '
          f'Hi, my name is {firstname}, my name is {name} '
          f'and I am your {role}!')
    
def get_name(ti):
    ti.xcom_push(key='lastname', value='Cop')
    ti.xcom_push(key='firstname', value='Christophe')



with DAG(
    default_args=default_args,
    dag_id='A_First_dag_python_operator_v02',
    description='Our first DAG python operator from the youtube tutorial',
    start_date=datetime(2025, 3, 6, 15), # Year, Month, Day, Hour, Minute, Second
     schedule_interval='@weekly'   
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=greet,
        op_kwargs={ 'role': 'Lecturer'}
    )
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

task2>>task1
