from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sys

sys.path.append('.')

"""
Defines and orchestrates the full data pipeline using Apache Airflow.
"""
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'customer_churn_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

# Define Python functions for tasks
def data_ingestion():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/1_data_ingestion.py"])

def raw_data_storage():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/2_data_ingestion.py"])

def data_validation():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/3_data_ingestion.py"])

def data_preparation():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/4_data_ingestion.py"])

def transform_data():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/5_dts.py"])

def feature_store():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/6_feature_store.py"])

def data_versioning():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/7_data_versioning.py"])

def train_model():
    subprocess.run(["python3", "/home/deepakn/dags/dmml/8_train_model.py"])

ingestion_task = PythonOperator(
    task_id='data_ingestion',
    python_callable=data_ingestion,
    op_args=[],
    dag=dag
)

raw_data_storage_task = PythonOperator(
    task_id='raw_data_storage',
    python_callable=raw_data_storage,
    op_args=[],
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=data_validation,
    op_args=[],
    dag=dag
)

prepare_task = PythonOperator(
    task_id='prepare_data',
    python_callable=data_preparation,
    op_args=[],
    dag=dag
)

transformation_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[],
    dag=dag
)

feature_store_task = PythonOperator(
    task_id='feature_store',
    python_callable=feature_store,
    op_args=[],
    dag=dag
)

data_versioning_task = PythonOperator(
    task_id='data_versioning',
    python_callable=data_versioning,
    op_args=[],
    dag=dag
)

train_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag
)

ingestion_task >> raw_data_storage_task >> validate_task >> prepare_task >> transformation_task >> feature_store_task >> data_versioning_task >> train_task

