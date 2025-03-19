FROM apache/airflow:2.10.5

USER airflow

# Install the MongoDB provider
RUN pip install apache-airflow-providers-mongo
RUN pip install pymongo
# RUN apt-get update && apt-get install -y docker.io

USER airflow
