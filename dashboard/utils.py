import os
import clickhouse_connect
from dotenv import load_dotenv


load_dotenv()

CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST_LOCAL')

def get_db_client():

    try:
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DB)
        return client
    except:
        print('error create clickhouse connection')

        return None

def get_forecast_data():
    
    client = get_db_client()
    df = client.query_df('select f.* , loc.provinsi, loc.kotkab, loc.kecamatan, loc.desa, date(f.local_datetime) as date, hour(f.local_datetime) as hour from forecasts f final left join master_locations loc on loc.adm4 = f.location_code where date(f.local_datetime) in (NOW(), (NOW() + INTERVAL 1 DAY), (NOW() + INTERVAL 2 DAY))')

    return df