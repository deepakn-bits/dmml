import pandas as pd
#import great_expectations as ge
#from pydeequ.checks import Check, CheckLevel
#from pydeequ.verification import VerificationSuite
import pandera as pa

#sys.path.append("../Data_Ingestion")

from data_ingestion import logging

#DATASET = '../Data_Ingestion/customer_churn_dataset-training-master.csv'
DATASET = 'customer_churn_dataset-training-master.csv'

def validate_data():
    """
    Validates the dataset for missing values, duplicates, and incorrect data types.
    """
    df = pd.read_csv(DATASET)

    df["Gender"].fillna("Unknown", inplace=True)
    df["Total Spend"].fillna(df["Total Spend"].median(), inplace=True)  # Use median value
    df["Subscription Type"].fillna("Unknown", inplace=True)
    df["Contract Length"].fillna("Unknown", inplace=True)

    # Replace NaN values with the median for all numerical columns
    numerical_columns = ["Age", "Tenure", "Usage Frequency", "Support Calls", 
                        "Payment Delay", "Total Spend", "Last Interaction", "Churn"]

    for col in numerical_columns:
        df[col].fillna(df[col].median(), inplace=True)

    df["Churn"] = df["Churn"].astype(int)  # Convert Churn column to int64


    # Define Pandera Schema
    schema = pa.DataFrameSchema({
        "Age": pa.Column(pa.Float, nullable=True),
        "Gender": pa.Column(pa.String, checks=pa.Check.isin(["Male", "Female", "Unknown"]), nullable=True),
        "Total Spend": pa.Column(pa.Float, checks=pa.Check.greater_than(0)),
        "Subscription Type": pa.Column(pa.String, nullable=False),
        "Contract Length": pa.Column(pa.String, nullable=False),
        "Tenure": pa.Column(pa.Float, checks=pa.Check.greater_than_or_equal_to(0)),
        "Usage Frequency": pa.Column(pa.Float, checks=pa.Check.greater_than_or_equal_to(0)),
        "Support Calls": pa.Column(pa.Float, checks=pa.Check.greater_than_or_equal_to(0)),
        "Payment Delay": pa.Column(pa.Float, checks=pa.Check.greater_than_or_equal_to(0)),
        "Last Interaction": pa.Column(pa.Float, checks=pa.Check.greater_than_or_equal_to(0)),
        "Churn": pa.Column(pa.Int, checks=pa.Check.isin([0, 1]))
    })
    
    try:
        # Validate Data
        df = schema.validate(df)
        print("Data validation passed successfully!")
    except pa.errors.SchemaErrors as e:
        print("Data validation failed with errors:")
        print(e.failure_cases)
        return None


validate_data()
