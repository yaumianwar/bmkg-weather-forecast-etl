from datetime import datetime, timedelta
from helpers.db_connect import get_clickhouse_connection
from helpers.get_forecasts import get_forecasts
from helpers.utils import parse_datetime, is_newest_version
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator

FORECAST_DB_COLUMNS = ['location_code', 'timezone', 'forecast_code', 'datetime', 't', 'tcc', 'tp', 'weather', 'weather_desc', 'weather_desc_en', 'wd_deg', 'wd', 'wd_to', 'ws', 'hu', 'vs', 'vs_text', 'time_index', 'analysis_date', 'image', 'utc_datetime', 'local_datetime']
DATETIME_KEYS = ['datetime', 'analysis_date', 'utc_datetime', 'local_datetime']

# Instantiate a DAG with @dag operator
@dag(
    start_date=datetime(2025, 3, 30),
    schedule="0 1,13 * * *", # run twice a day at 01.00 UTC and 13.00 UTC
    catchup=False,
    tags=["etl", "forecast"],
)
def etl_forecast():

    # define extract, transform and load task

    def extract():
        
        # get forecasts data
        data = get_forecasts()
        if len(data) == 0:
            raise AirflowException("No data fetched from the API. Retrying...")

        # check if the data we got is the newest version
        if not is_newest_version(data[0]['analysis_date']):
            raise AirflowException("Data is not the newest version. Retrying...")

        return data

    # extract task that will re-run in the next 30 minutes when is_newest_version() return False value
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
        retries=3,
        retry_delay=timedelta(minutes=30),
    )
    
    def transform(ti):
        forecasts_data = ti.xcom_pull(task_ids="extract_task")

        # convert datetime column value from string to datetime
        for forecast_data in forecasts_data:
            for key in DATETIME_KEYS:
                forecast_data.update({key: parse_datetime(forecast_data[key])})

        #transform array of dict value to array of array
        data = [list(d.values()) for d in forecasts_data]

        return data
    
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
        provide_context=True,
    )

    
    def load(ti):
        forecast_data = ti.xcom_pull(task_ids="transform_task")

        # get clickhouse connection
        client = get_clickhouse_connection()
        
        # insert data to clickhouse
        client.insert(
            'forecasts', 
            forecast_data, 
            column_names= FORECAST_DB_COLUMNS
        )

        print(f"Insert forecasts data to clickhouse success. Total forecasts data is: {len(forecast_data)}")

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
        provide_context=True,
    )   

    # Main flow of the DAG
    extract_task >> transform_task >> load_task 


etl_forecast()
