from datetime import datetime, timedelta
from helpers.db_connect import get_clickhouse_connection

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
    # get clickhouse connection
    client = get_clickhouse_connection()
    
    # get the latest analysis date from existing data
    query_result = client.query('select max(analysis_date) from forecasts')
    current_analysis_date = query_result.result_rows[0][0]

    print(f'current_analysis_date {current_analysis_date}. new_analysis_date {new_analysis_date}')

    if current_analysis_date >= datetime.strptime(new_analysis_date, '%Y-%m-%dT%H:%M:%S'):
        return False
    
    return True
