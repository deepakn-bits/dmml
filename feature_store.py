import pandas as pd
import sqlite3

from data_ingestion import logging

DATASET = 'customer_churn_dataset-training-master.csv'

def create_feature_store():
    """
    Validates, transforms, and stores the dataset for further processing.
    """
    df = pd.read_csv(DATASET)
    
    # Store transformed data in a database
    conn = sqlite3.connect("customer_churn.db")
    df.to_sql("transformed_data", conn, if_exists="replace", index=False)

    # Define and store feature metadata
    feature_metadata = pd.DataFrame({
        "Feature Name": df.columns,
        "Description": [
            "Unique customer identifier", "Customer age", "Gender of the customer",
            "Customer tenure in months", "Frequency of usage", "Number of support calls made",
            "Number of delayed payments", "Type of subscription", "Contract length category",
            "Total spend by customer", "Days since last interaction", "Churn label (0 = Retained, 1 = Churned)"
        ],  # Now exactly 12 elements
        "Source": ["Raw" if col in [
            'CustomerID', 'Age', 'Gender', 'Tenure', 'Usage Frequency',
            'Support Calls', 'Payment Delay', 'Subscription Type',
            'Contract Length', 'Total Spend', 'Last Interaction', 'Churn'] else "Engineered"
            for col in df.columns],
        "Version": ["1.0"] * len(df.columns)
    })

    feature_metadata.to_sql("feature_metadata", conn, if_exists="replace", index=False)
    conn.close()
    
    logging.info(f"Feature storage completed")
    return df

def retrieve_features():
    """
    Retrieves stored features for training or inference.
    """
    conn = sqlite3.connect("customer_churn.db")
    df_features = pd.read_sql("SELECT * FROM transformed_data", conn)
    conn.close()
    logging.info('FEatures retrieval done')
    return df_features

create_feature_store()
df_features = retrieve_features()
logging.info(f"Features: {df_features}")
