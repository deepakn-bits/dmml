
'''
This module provides functionality to store data in an S3 bucket using AWS credentials.
Functions:
    store_data(): Stores data to an S3 bucket.
    set_env_vars(): Sets AWS credentials as environment variables from a file.
Usage:
    This script is intended to be run as a standalone module. It initializes AWS credentials, 
    and uploads a specified dataset to an S3 bucket.
Example:
    $ python raw_data_storage.py
'''

# pylint: disable=import-error, broad-exception-caught, wrong-import-position
import os
from pathlib import Path
import sys

# Third party imports
import boto3

sys.path.append("../Data_Ingestion")

# Local imports
from data_ingestion import logging

DATA_SET = "customer_churn_dataset-training-master.csv"

def store_data():
    """
    Stores a dataset file to an S3 bucket.
    This function initializes an S3 client, checks if the dataset file exists,
    and uploads it to a specified S3 bucket. If the file does not exist or
    there is an error during the upload process, the function logs an error
    message and exits the program.
    Exits:
        - Exits the program if the dataset file does not exist.
        - Exits the program if there is an error during the upload process.
    """

    # Initialize the S3 client
    logging.info("Initializing S3 client.")
    s3 = boto3.client("s3", region_name="ap-south-1")

    # Define the bucket name and file path
    bucket_name = "bitschurndataset"
    file_path = f"../Data_Ingestion/{DATA_SET}"

    logging.info(f"S3 Bucket Name:{bucket_name} dataset={file_path}")

    my_file = Path(file_path)
    if not my_file.is_file():
        logging.error(f"{file_path} file doesn't exist. Exiting")
        sys.exit(1)

    s3_key = DATA_SET
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        sys.exit(1)

    logging.info("Dataframe is saved as csv in S3 bucket")


def set_env_vars():
    """
    Reads AWS access keys from a file and sets them as environment variables.
    Raises:
        FileNotFoundError: If the 'keys.txt' file does not exist.
        IndexError: If the 'keys.txt' file does not contain at least two lines.
    """

    with open('../keys.txt', encoding='utf-8') as kh:
        keys = kh.readlines()

    #Load AWS keys to env varaibles
    os.environ['AWS_ACCESS_KEY_ID'] = keys[0].strip('\n')
    os.environ['AWS_SECRET_ACCESS_KEY'] = keys[1].strip('\n')

    logging.info(f"aws_access_key_id {keys[0]}")
    logging.info(f"aws_secret_access_key {keys[1]}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        set_env_vars()
        store_data()
        logger.info("Data stored successfully.")
    except Exception as e:
        logger.error(f"Failed to store data: {e}")
