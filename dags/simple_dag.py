from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# DAG definition
# This DAG is a simple example that runs daily and has three tasks
dag = DAG(
    'simple_dag',
    start_date=datetime(2025, 12, 1),
    schedule_interval='@yearly', 
    description='Simple DAG example',
)

# Task 1: Get data from external source
def extract():
    print("get data from external source...")

# Task 2: Data transformation
def transfrom():
    print("process and transform data...")

# Task 3: Load data into database
def load():
    print("Load data into database/data warehouse...")

# Create and register tasks into dag
task1 = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

task2 = PythonOperator(
    task_id='transfrom', 
    python_callable=transfrom,
    dag=dag
)

task3 = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# Task dependencies
task1 >> task2 >> task3