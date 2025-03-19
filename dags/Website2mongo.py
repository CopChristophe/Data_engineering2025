from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Import this
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

def fetch_data_from_websites(urls):
    """Fetch data from multiple URLs and return a dictionary."""
    data = {}
    for url in urls:
        response = requests.get(url)
        if response.status_code == 200:
            data[url] = response.text
        else:
            data[url] = f"Failed to retrieve data. Status: {response.status_code}"
    return data

def save_data_to_mongo(ti):
    """Save scraped data to MongoDB."""
    # Retrieve scraped data from XCom
    scraped_data = ti.xcom_pull(task_ids="fetch_data")

    # Connect to MongoDB (use container name, not localhost)
    mongo_client = MongoClient(host="mongo-db", port=27017, username="root", password="example")
    db = mongo_client["files_db"]
    files_collection = db["files"]

    file_ids = []  # List to store the MongoDB _id values of the inserted documents

    for url, content in scraped_data.items():
        file_data = {
            "url": url,
            "content": content
        }
        # Insert data and get the inserted document's _id
        result = files_collection.insert_one(file_data)
        file_ids.append(str(result.inserted_id))  # Convert ObjectId to string before appending
        print(f"Data from {url} saved to MongoDB with ID: {str(result.inserted_id)}")

    # Push the list of file IDs to XCom
    ti.xcom_push(key='file_ids', value=file_ids)

    print("All data have been saved to MongoDB.")

# Define the DAG
with DAG(
    dag_id='web_scraping_dag',
    default_args=default_args,
    description='DAG to scrape data from a website and save to MongoDB',
    start_date=datetime(2025, 3, 6, 15),
    schedule_interval='@weekly'
) as dag:

    urls = [
        'https://dbpedia.org/class/yago/WikicatLogicalFallacies',
        'https://en.wikipedia.org/wiki/List_of_fallacies',
        'https://en.wikipedia.org/wiki/List_of_cognitive_biases',
        'https://www.wikidata.org/wiki/Q2607828',
        'https://simple.wikipedia.org/wiki/Category:Logical_fallacies'
    ]

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_websites,
        op_args=[urls]
    )

    save_data = PythonOperator(
        task_id='save_data',
        python_callable=save_data_to_mongo,
        provide_context=True  # Ensures access to task instance (ti)
    )

    trigger_parse_dag = TriggerDagRunOperator(
        task_id='trigger_parse_dag',
        trigger_dag_id='parse_mongo_to_sql_dag',  # Make sure this matches the actual DAG ID of your second DAG
        wait_for_completion=True  # Ensures it waits for completion before marking as done (optional)
    )

    # Define task dependencies
    fetch_data >> save_data >> trigger_parse_dag
