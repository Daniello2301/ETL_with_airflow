from pandas import DataFrame
from typing import Dict
from sqlalchemy import create_engine

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "project", "src"))

from project.src.transform import run_queries
from scripts.createDB import create_db

def transform_data() -> Dict[str, DataFrame]:
    """This function transforms the data in the SQLite database.
    """

    # 1. Create the database connection
    db_path = create_db()
    # Create the SQLite engine
    engine = create_engine(f"sqlite:///{db_path}")

    # create the data folder if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # 1. Run the queries to transform the data
    query_results = run_queries(database=engine)


    # 2. Show  the results of the queries
    for table_name, df in query_results.items():
        # Show the each table name and its type
        print(f"Table: {table_name}")
        print(type(df))

    print("Data transformed successfully!")
    return query_results