from project.src.load import load
from scripts.createDB import create_db
from scripts.extract import extract_data

from sqlalchemy import create_engine


def etl_load_data():
    """This function runs the ETL process to extract data from CSV files and load it into a SQLite database.
    """
    # 1. Create the database and tables
    db_path = create_db()
    # Create the SQLite engine
    engine =  create_engine(f"sqlite:///{db_path}")

    # 2. Extract the data from the CSV files and public holidays
    csv_dataframes = extract_data()

    # 3. Load the data into the SQLite database
    load(data_frames=csv_dataframes, database=engine)

    return "Data loaded successfully!"