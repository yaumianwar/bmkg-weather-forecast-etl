import json
import glob
import os
import logging
import requests
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime

# DAG definition
# This DAG is a simple example that runs daily and has three tasks
@dag(
    'simple_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@weekly', 
    catchup=False,
    description='Simple DAG example',
)
def simple_dag():

    # Task 1: Get data from external source
    def extract():
        print("get data from external source...")
        url = 'https://jsonplaceholder.typicode.com/posts'
        # Make a GET request to the API endpoint using requests.get()
        response = requests.get(url)
        data = []

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()
        
        return {
            'data': data,
            'total_data': len(data)
        }
        
    # Task 2: Data transformation
    def transform(**context):
        print("process and transform data...")

        extract_info = context['task_instance'].xcom_pull(task_ids='extract')
        data = extract_info['data']
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = [
            {**item, 'created_at': created_at}
            for item in data
        ]

        data_dir = f'/tmp'
        os.makedirs(data_dir, exist_ok=True)
        file_path = os.path.join(data_dir, f"transformed_data{context['run_id']}.json")

        # Save data to file
        with open(file_path, 'w') as f:
            json.dump(transformed_data, f, indent=2)
        
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
        logging.info(f"Data saved to {file_path}, size: {file_size:.2f} MB")
        logging.info(f"Total records: {len(transformed_data)}")
        
        # Return file path via XCom (small data)
        return {
            'file_path': file_path,
            'record_count': len(transformed_data),
            'file_size_mb': file_size
        }

    # Task 3: Load data into database
    def load(**context):
        transform_info = context['task_instance'].xcom_pull(task_ids='transform')
        file_path = transform_info['file_path']

        with open(file_path, 'r') as f:
            transformed_data = json.load(f)

        print(f"Total transformed data: {len(transformed_data)}")
        print("First 5 records: ", transformed_data[:5])

        return "Data loaded successfully"

    def cleanup_files(**context):

        try:
            # Get file paths from previous tasks
            transform_info = context['task_instance'].xcom_pull(task_ids='transform')
            file_path = transform_info['file_path']

            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"Cleaned up file: {file_path}")
            
            return "Cleanup completed successfully"
            
        except Exception as e:
            logging.warning(f"Cleanup warning: {str(e)}")
            return "Cleanup completed with warnings"

    # Create and register tasks into dag
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform', 
        python_callable=transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files,
        trigger_rule='all_done',  # Run even if upstream tasks fail
    )

    # Task dependencies
    extract_task >> transform_task >> load_task >> cleanup_task

simple_dag()