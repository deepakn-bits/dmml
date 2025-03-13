import pandas as pd
import sqlite3

from data_ingestion import logging

DATASET = 'customer_churn_dataset-training-master.csv'

def transform_and_store():
    """
    Validates, transforms, and stores the dataset for further processing.
    """
    df = pd.read_csv(DATASET)
    
    # Feature Engineering
    df['Avg_Spend_Per_Interaction'] = df['Total Spend'] / (df['Last Interaction'] + 1)
    df['Support_Call_Rate'] = df['Support Calls'] / (df['Tenure'] + 1)

    # Store transformed data in a database
    conn = sqlite3.connect("customer_churn.db")
    df.to_sql("transformed_data", conn, if_exists="replace", index=False)
    conn.close()

    logging.info(f"Data transformation and storage completed.")

    return df

# Example usage
df_cleaned = transform_and_store()
