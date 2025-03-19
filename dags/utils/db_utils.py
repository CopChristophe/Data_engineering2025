from sqlalchemy import create_engine
import pandas as pd

def store_to_postgres(df, table_name):
    """Stores the parsed data into PostgreSQL."""
    db_user = "airflow"
    db_password = "airflow"
    db_host = "postgres"
    db_port = "5432"
    db_name = "airflow"

    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    print(f"Data uploaded to PostgreSQL table {table_name}.")
