from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag',
    default_args=default_args, 
    description='Our first DAG from the youtube tutorial',
    start_date=datetime(2025, 3, 6, 15), # Year, Month, Day, Hour, Minute, Second
    schedule_interval='@daily'
    ) as dag:
        task1 = BashOperator(
            task_id='first_task',
            bash_command='echo first task! This is something you should see in the logs'
        )

        task2 = BashOperator(
            task_id='second_task',
            bash_command='echo I am task two, but it failed somehow?' #issues with the '' vs ""
        )

        task3 = BashOperator(
            task_id='third_task',
            bash_command='echo I am the third task, and I am here to save the day!'
        )

task1.set_downstream(task2)  # This is how you set the dependencies between tasks, but is cumbersome
task1.set_downstream(task3)

# task1 >> task2 >> task3  # This is another way to set the dependencies between tasks 
# task >> [task2,task3]  # This is how you set the dependencies between tasks
