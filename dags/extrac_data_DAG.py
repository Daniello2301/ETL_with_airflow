from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract_data


default_args = {
    'owner': 'airflow',
}


with DAG(
    dag_id='extract_data_dag',
    default_args=default_args,
    start_date=datetime(2023, 10, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ETL', 'CSV', 'sqlite'],
) as dag:
    # Define the task to extract data
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    # Set dependencies between tasks
    extract_task 