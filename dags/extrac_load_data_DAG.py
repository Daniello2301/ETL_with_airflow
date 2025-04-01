# DAF for extracting and loading data from a CSV file into a SQLite database
#
# This DAG extracts data from CSV files and loads it into a SQLite database.
# It uses the Airflow framework to schedule and manage the ETL process.
# The DAG is set to run if want.
#
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "project", "src"))

from scripts.extract import extract_data
from scripts.createDB import create_db
from project.src.load import load


def etl_load_data():
    """This function runs the ETL process to extract data from CSV files and load it into a SQLite database.
    """
    # 1. Create the database and tables
    engine = create_db()

    # 2. Extract the data from the CSV files and public holidays
    csv_dataframes = extract_data()

    # 3. Load the data into the SQLite database
    load(data_frames=csv_dataframes, database=engine)

    return "ETL process completed successfully!"


# Definir DAG en Airflow
default_args = {
    "owner": "airflow", 
    "start_date": datetime(2024, 3, 31)
}

with DAG(
    dag_id="etl_dag_1",
    default_args=default_args,
    schedule_interval=None,  # Set to None to run manually
    catchup=False,
) as dag:
    # Define the task to run the ETL process
    etl_task = PythonOperator(
        task_id="etl_load_data",
        python_callable=etl_load_data,
    )

    # Set dependencies between tasks (if needed)
    etl_task
