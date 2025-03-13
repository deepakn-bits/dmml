'''Data ingestion from two data sources'''

import logging
import os
import pandas as pd
import kaggle
import sqlite3
import pandas.io.sql as sql

# One level up from current directory
#BASE_DIR = os.path.dirname(os.getcwd())
#DATA_PATH = os.path.join(BASE_DIR , "Data_Ingestion")
#FILE_NAME = os.path.join(DATA_PATH , "customer_churn_dataset-training-master.csv")
FILE_NAME = "customer_churn_dataset-training-master.csv"


def read_data_from_db():
    """
    Reads data from a SQLite database and writes it to a CSV file.

    This function connects to a SQLite database named 'database.db', reads data from a table named 'some_table',
    and writes the data to a CSV file named 'customer_churn_dataset-testing-master.csv'.

    Returns:
        None
    """
    #con = sqlite3.connect("D:\Software\sqlite-tools-win-x64-3490100\churn.db")
    con = sqlite3.connect("churn.db")
    table = pd.read_sql_query('select * from customer_churn', con)
    table.to_csv(FILE_NAME)
    logging.info("Successfully read data from SQLite DB into a CSV file.")

def read_data_by_rest_api():
    """
    Downloads a dataset from Kaggle using the Kaggle API and reads it into a pandas DataFrame.
    This function downloads the specified dataset from Kaggle, unzips it, and reads the CSV file
    into a pandas DataFrame. It prints the first 5 records of the DataFrame. If an error occurs
    during the process, it prints an error message.
    Parameters:
    None
    Returns:
    None
    Raises:
    Exception: If there is an error during the download or reading of the dataset.
    """
    logging.info("Starting to download and read data from Kaggle API.")
    dataset = "muhammadshahidazeem/customer-churn-dataset"

    try:
        # Download the dataset
        kaggle.api.dataset_download_files(dataset, path='.', unzip=True)
        logging.info("Successfully downloaded and unzipped dataset from Kaggle.")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(FILE_NAME)
        logging.info("Successfully read data from Kaggle dataset.")
        print("First 5 records:", df.head())
    except Exception as err: # pylint: disable=broad-exception-caught
        logging.error("An error occurred: %s", err)

def ingest_data():
    """
    Ingests data from various sources and logs the process.
    This function configures logging to output to both a file and the console.
    It logs the start and completion of the data ingestion process and calls
    functions to read data from CSV files and REST APIs.
    Functions called:
    - read_data_by_csv: Reads data from CSV files.
    - read_data_by_rest_api: Reads data from REST APIs.
    Logging:
    - Logs are written to 'data_ingestion.log' and the console with INFO level.
    """

    # Configure logging
    logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s - %(levelname)s - %(message)s',
      handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
      ]
    )

    logging.info("Starting data ingestion process.")
    read_data_from_db()
    read_data_by_rest_api()
    logging.info("Data ingestion process completed.")

if __name__ == "__main__":
    ingest_data()
