import pandas as pd
import os
import dvc.api

from data_ingestion import logging

DATASET = 'customer_churn_dataset-training-master.csv'

def data_versioning():
    '''Perform data versioning, save the transformed dataset, and generate a data quality report.
    This function performs the following steps:
    1. Saves the transformed dataset to a specified path.
    2. Adds the transformed dataset to DVC (Data Version Control) for versioning.
    3. Commits the changes to Git.
    4. Generates a data quality report and saves it to a specified path.
    Returns:
        pd.DataFrame: The transformed dataset.
    # Function implementation
    This function connects to a SQLite database, retrieves the transformed data,
    and returns it as a pandas DataFrame.
    Returns:
        pd.DataFrame: The retrieved features from the database.
    '''
    df = pd.read_csv(DATASET)

    # Save versioned data
    versioned_data_path = "data/processed/transformed_data.csv"
    os.makedirs(os.path.dirname(versioned_data_path), exist_ok=True)
    df.to_csv(versioned_data_path, index=False)
    os.system("dvc add data/processed/transformed_data.csv")
    os.system("git add data/processed/transformed_data.csv.dvc")
    os.system("git commit -m 'Versioned transformed dataset' && git push")
    
    logging.info(f"Data versioning completed.")

data_versioning()