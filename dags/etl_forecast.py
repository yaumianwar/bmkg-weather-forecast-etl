from datetime import datetime, timedelta
from helpers.db_connect import get_clickhouse_connection
from helpers.get_forecasts import get_forecasts
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator

FORECAST_DB_COLUMNS = ['location_code', 'timezone', 'forecast_code', 'datetime', 't', 'tcc', 'tp', 'weather', 'weather_desc', 'weather_desc_en', 'wd_deg', 'wd', 'wd_to', 'ws', 'hu', 'vs', 'vs_text', 'time_index', 'analysis_date', 'image', 'utc_datetime', 'local_datetime']
DATETIME_KEYS = ['datetime', 'analysis_date', 'utc_datetime', 'local_datetime']

# get clickhouse connection
client = get_clickhouse_connection()

# Function to convert string to datetime
def parse_datetime(datetime_str):
    formats = ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
    
    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse datetime: {datetime_str}")

def is_newest_version(new_analysis_date):
    # get the latest analysis date from existing data
    query_result = client.query('select max(analysis_date) from forecasts')
    current_analysis_date = query_result.result_rows[0][0]

    print(f'current_analysis_date {current_analysis_date}. new_analysis_date {new_analysis_date}')

    if current_analysis_date >= datetime.strptime(new_analysis_date, '%Y-%m-%dT%H:%M:%S'):
        return False
    
    return True

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
        
        # check if clickhouse client succesfully connect
        if client is None:
            return

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
        print('forecast_data', forecast_data)
        
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
