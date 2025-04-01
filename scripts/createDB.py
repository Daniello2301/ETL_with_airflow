from pathlib import Path

from project.src import config

def create_db():
    """This function creates the database and the tables in it.
    """
    # 1. Create the database sql file
    Path(config.SQLITE_BD_ABSOLUTE_PATH).touch(exist_ok=True)

    print("Database created successfully!")
    # 2. Create the engine to connect to the database
    return config.SQLITE_BD_ABSOLUTE_PATH

    