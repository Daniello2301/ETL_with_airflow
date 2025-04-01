from project.src import config
from project.src.extract import extract




def extract_data():
    
    csv_folder = config.DATASET_ROOT_PATH
    public_holidays_url = config.PUBLIC_HOLIDAYS_URL

    # 1. Get the mapping of the csv files to the table names.
    csv_table_mapping = config.get_csv_to_table_mapping()

    # 2. Extract the data from the csv files, holidays and load them into the dataframes.
    csv_dataframes = extract(csv_folder, csv_table_mapping, public_holidays_url)
    
    print("Data extracted successfully!")
    return csv_dataframes