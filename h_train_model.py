import pandas as pd
import great_expectations as ge
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import os
import dvc.api

def train_model():
    """
    Trains a machine learning model to predict customer churn and logs it with MLflow.
    """
    conn = sqlite3.connect("customer_churn.db")
    df = pd.read_sql("SELECT * FROM transformed_data", conn)
    conn.close()
    
    # Define features and target
    X = df.drop(columns=['Churn', 'CustomerID'])
    y = df['Churn']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    with mlflow.start_run():
        # Train model
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Evaluate model
        y_pred = model.predict(X_test)
        metrics = {
            "Accuracy": accuracy_score(y_test, y_pred),
            "Precision": precision_score(y_test, y_pred),
            "Recall": recall_score(y_test, y_pred),
            "F1 Score": f1_score(y_test, y_pred)
        }
        
        # Log parameters and metrics to MLflow
        mlflow.log_params({"n_estimators": 100, "random_state": 42})
        mlflow.log_metrics(metrics)
        
        # Log model
        mlflow.sklearn.log_model(model, "customer_churn_model")
        
        # Save the model locally
        joblib.dump(model, "customer_churn_model.pkl")
        
    logging.info("Model training completed. Metrics:", metrics)
    return metrics

train_model()