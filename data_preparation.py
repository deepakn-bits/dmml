import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns

from data_ingestion import logging

DATASET = 'customer_churn_dataset-training-master.csv'

def prepare_data():
    """
    Prepares the dataset for further processing.
    """
    df = pd.read_csv(DATASET)
    
    # Handle missing values
    df.dropna(inplace=True)
    
    # Encode categorical variables
    label_encoders = {}
    categorical_columns = ['Gender', 'Subscription Type', 'Contract Length']
    for col in categorical_columns:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
        label_encoders[col] = le
    
    # Normalize numerical features
    numerical_columns = ['Age', 'Tenure', 'Usage Frequency', 'Support Calls', 'Payment Delay', 'Total Spend', 'Last Interaction']
    scaler = StandardScaler()
    df[numerical_columns] = scaler.fit_transform(df[numerical_columns])
    
    # Data visualization
    plt.figure(figsize=(10, 6))
    sns.histplot(df['Total Spend'], bins=50, kde=True)
    plt.title("Distribution of Total Spend")
    plt.show()

    logging.info(f"Data preparation completed.")

    return df

# Example usage
df_cleaned = prepare_data()
