import os
import clickhouse_connect
from dotenv import load_dotenv
from pathlib import Path

# load env variable
load_dotenv()

CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')

def get_clickhouse_connection():
    
    try:
        client = clickhouse_connect.get_client(host='clickhouse', username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DB)
        return client
    
    except:
        print('error create clickhouse connection')

        return None
    
