from datetime import datetime
from helpers.db_connect import get_clickhouse_connection
from helpers.get_forecasts import get_forecasts
from airflow.decorators import dag, task

FORECAST_DB_COLUMNS = ['location_code', 'timezone', 'forecast_code', 'datetime', 't', 'tcc', 'tp', 'weather', 'weather_desc', 'weather_desc_en', 'wd_deg', 'wd', 'wd_to', 'ws', 'hu', 'vs', 'vs_text', 'time_index', 'analysis_date', 'image', 'utc_datetime', 'local_datetime']
DATETIME_KEYS = ['datetime', 'analysis_date', 'utc_datetime', 'local_datetime']

# Function to convert string to datetime
def parse_datetime(datetime_str):
    formats = ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
    
    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse datetime: {datetime_str}")

# Instantiate a DAG with @dag operator
@dag(
    start_date=datetime(2025, 3, 30),
    schedule="30 1,13 * * *", # run twice a day at 01.30 UTC and 13.30 UTC
    catchup=False,
    tags=["etl", "forecast"],
)
def etl_forecast():
    # define extract, transform and load task

    @task()
    def extract():
        
        # get forecasts data
        data = get_forecasts()
        
        return data
    
    @task()
    def transform(forecasts_data: list):

        # convert datetime column value from string to datetime
        for forecast_data in forecasts_data:
            for key in DATETIME_KEYS:
                forecast_data.update({key: parse_datetime(forecast_data[key])})

        #transform array of dict value to array of array
        data = [list(d.values()) for d in forecasts_data]

        return data

    
    @task()
    def load(forecast_data: list):
        
        # create clickhouse connection
        client = get_clickhouse_connection()
        if client is None:
            return
        
        # insert data to clickhouse
        client.insert(
            'forecasts', 
            forecast_data, 
            column_names= FORECAST_DB_COLUMNS
        )

        print(f"Insert forecasts data to clickhouse success. Total forecasts data is: {len(forecast_data)}")
        

    # Main flow of the DAG
    raw_forecast_data = extract()
    forecast_data = transform(raw_forecast_data)
    load(forecast_data)


etl_forecast()
