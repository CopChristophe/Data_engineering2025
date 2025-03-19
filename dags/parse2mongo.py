from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.mongo_utils import fetch_from_mongo  # Custom function to fetch from MongoDB
from utils.parsing_utils import parse_html     # Custom function to parse HTML content
from utils.db_utils import store_to_postgres  # Custom function to store data in PostgreSQL
import pandas as pd  # Ensure pandas is imported for DataFrame manipulation

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

def fetch_fallacies(**kwargs):
    ti = kwargs['ti']
    file_contents = fetch_from_mongo("https://en.wikipedia.org/wiki/List_of_fallacies")
    # print(f"Fetched file contents for fallacies: {file_contents}")
    ti.xcom_push(key='file_contents_fallacies', value=file_contents)

def fetch_biases(**kwargs):
    ti = kwargs['ti']
    file_contents = fetch_from_mongo("https://en.wikipedia.org/wiki/List_of_cognitive_biases")
    # print(f"Fetched file contents for biases: {file_contents}")
    ti.xcom_push(key='file_contents_biases', value=file_contents)

def parse_fallacies(**kwargs):
    ti = kwargs['ti']
    file_contents = ti.xcom_pull(task_ids='fetch_fallacies', key='file_contents_fallacies')

    if not file_contents:
        raise ValueError("No content fetched from MongoDB.")

    df = parse_html(file_contents, "https://en.wikipedia.org/wiki/List_of_fallacies")
    parsed_data = df.to_dict(orient='records')
    ti.xcom_push(key='parsed_data_fallacies', value=parsed_data)

def parse_biases(**kwargs):
    ti = kwargs['ti']
    file_contents = ti.xcom_pull(task_ids='fetch_biases', key='file_contents_biases')

    if not file_contents:
        raise ValueError("No content fetched from MongoDB.")

    df = parse_html(file_contents, "https://en.wikipedia.org/wiki/List_of_cognitive_biases")
    parsed_data = df.to_dict(orient='records')
    ti.xcom_push(key='parsed_data_biases', value=parsed_data)

def store_fallacies(**kwargs):
    ti = kwargs['ti']
    parsed_data = ti.xcom_pull(task_ids='parse_fallacies', key='parsed_data_fallacies')

    if not parsed_data:
        raise ValueError("No parsed data to store in PostgreSQL.")

    df = pd.DataFrame(parsed_data)
    store_to_postgres(df, "logical_fallacies")

def store_biases(**kwargs):
    ti = kwargs['ti']
    parsed_data = ti.xcom_pull(task_ids='parse_biases', key='parsed_data_biases')

    if not parsed_data:
        raise ValueError("No parsed data to store in PostgreSQL.")

    df = pd.DataFrame(parsed_data)
    store_to_postgres(df, "cognitive_biases")

with DAG(
    default_args=default_args,
    dag_id='parse_mongo_to_sql_dag',
    description='DAG to parse MongoDB data and store in PostgreSQL',
    start_date=datetime(2025, 3, 6, 15),
    schedule_interval=None  # Trigger manually or via another DAG
) as dag:

    signal_task = BashOperator(
        task_id='signal_message',
        bash_command='echo "I got the signal from the parsers, I can go fetch the mongo data now"'
    )

    fetch_fallacies_task = PythonOperator(
        task_id='fetch_fallacies',
        python_callable=fetch_fallacies,
        provide_context=True
    )

    fetch_biases_task = PythonOperator(
        task_id='fetch_biases',
        python_callable=fetch_biases,
        provide_context=True
    )

    parse_fallacies_task = PythonOperator(
        task_id='parse_fallacies',
        python_callable=parse_fallacies,
        provide_context=True
    )

    parse_biases_task = PythonOperator(
        task_id='parse_biases',
        python_callable=parse_biases,
        provide_context=True
    )

    store_fallacies_task = PythonOperator(
        task_id='store_fallacies',
        python_callable=store_fallacies,
        provide_context=True
    )

    store_biases_task = PythonOperator(
        task_id='store_biases',
        python_callable=store_biases,
        provide_context=True
    )

    signal_task >> fetch_fallacies_task >> parse_fallacies_task >> store_fallacies_task
    signal_task >> fetch_biases_task >> parse_biases_task >> store_biases_task