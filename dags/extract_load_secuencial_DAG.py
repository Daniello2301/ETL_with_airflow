from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "project", "src"))

from scripts.extract import extract_data
from scripts.createDB import create_db
from scripts.load import etl_load_data
from scripts.transform import transform_data

# Definir DAG en Airflow
default_args = {
    "owner": "airflow", 
    "start_date": datetime(2024, 3, 31)
}

with DAG(
    dag_id="etl_dag_2",
    default_args=default_args,
    schedule_interval=None,  # Set to None to run manually
    catchup=False,
    tags=["ETL", "CSV", "sqlite"],
) as dag:
    
    # Define the task to create database
    create_db_task = PythonOperator(
        task_id="create_db",
        python_callable=create_db,
    )

    # Define the task to extract data
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )    

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=etl_load_data,
        provide_context=True, # to pass the context to the function
    )

    # Define the task to transform data
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Set dependencies between tasks
    create_db_task >> extract_task >> load_task >> transform_task