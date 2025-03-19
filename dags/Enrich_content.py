from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from pymongo import MongoClient
from bs4 import BeautifulSoup
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

def fetch_bias_names_from_postgres():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    query = 'SELECT "Bias Name" FROM logical_fallacies'  # Use the correct column name with quotes
    df = pd.read_sql(query, engine)
    return df['Bias Name'].tolist()  # Ensure this matches the column name

def fetch_html_from_mongo(bias_name):
    mongo_client = MongoClient(host="mongo-db", port=27017, username="root", password="example")
    db = mongo_client["files_db"]
    collection = db["files"]
    file_data = collection.find_one({"content": {"$regex": bias_name}})
    return file_data["content"] if file_data else None

def enrich_biases(**kwargs):
    ti = kwargs['ti']
    bias_names = ti.xcom_pull(task_ids='fetch_bias_names')

    enriched_data = []

    for bias_name in bias_names:
        html_content = fetch_html_from_mongo(bias_name)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            li = soup.find('a', title=bias_name).find_parent('li')
            if li:
                a_tag = li.find('a', href=True)
                if a_tag:
                    bias_link = f"https://en.wikipedia.org{a_tag['href']}"
                    bias_explanation = li.get_text().split('–', 1)[-1].strip()
                    enriched_data.append({
                        'name': bias_name,
                        'link': bias_link,
                        'explanation': bias_explanation
                    })

    ti.xcom_push(key='enriched_data', value=enriched_data)

def update_postgres_with_enriched_data(**kwargs):
    ti = kwargs['ti']
    enriched_data = ti.xcom_pull(task_ids='enrich_biases', key='enriched_data')

    if not enriched_data:
        raise ValueError("No enriched data to store in PostgreSQL.")

    df = pd.DataFrame(enriched_data)
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    df.to_sql('cognitive_biases', engine, if_exists='replace', index=False)

with DAG(
    default_args=default_args,
    dag_id='enrich_content_dag',
    description='DAG to enrich cognitive biases with links and explanations',
    start_date=datetime(2025, 3, 6, 15),
    schedule_interval=None  # Trigger manually or via another DAG
) as dag:

    fetch_bias_names_task = PythonOperator(
        task_id='fetch_bias_names',
        python_callable=fetch_bias_names_from_postgres,
        provide_context=True
    )

    enrich_biases_task = PythonOperator(
        task_id='enrich_biases',
        python_callable=enrich_biases,
        provide_context=True
    )

    update_postgres_task = PythonOperator(
        task_id='update_postgres',
        python_callable=update_postgres_with_enriched_data,
        provide_context=True
    )

    fetch_bias_names_task >> enrich_biases_task >> update_postgres_task
