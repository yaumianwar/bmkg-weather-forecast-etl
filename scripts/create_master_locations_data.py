import os
import pandas as pd
import requests
import clickhouse_connect

from dotenv import load_dotenv

# load env variable
load_dotenv()

CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')

def get_clickhouse_connection():
    
    try:
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DB)

        return client
    
    except:
        print('error create clickhouse connection')

        return None

def create_master_locations_data():

    # make clickhouse connection
    client = get_clickhouse_connection()
    if client is None:
        return


    # get all district code (adm3) at Bulukumba Regency (73.02....)
    location_df = pd.read_csv('https://raw.githubusercontent.com/kodewilayah/permendagri-72-2019/main/dist/base.csv', header=None)
    location_df.rename(columns={0: "location_code", 1: "location_name"}, inplace=True)
    location_df = location_df[(location_df["location_code"].str.len() == 8) & (location_df["location_code"].str.startswith("73.02"))]
    location_codes = location_df["location_code"].values.tolist()

    # iterate each district to get list of sub-district
    location_data = []
    for location_code in location_codes:
        response = requests.get("https://api.bmkg.go.id/publik/prakiraan-cuaca?adm3={}".format(location_code))
        response_data = response.json()['data']
        for data in response_data:
            location_data.append(data['lokasi'])

    final_data = [list(value.values()) for value in location_data]

    # insert location data to master_location table
    client.insert(
        'master_locations', 
        final_data, 
        column_names=['adm1', 'adm2', 'adm3', 'adm4', 'provinsi', 'kotkab', 'kecamatan', 'desa', 'lot', 'lang', 'timezone', 'tipe']
    )
    
    print('create master location data success')

    return


create_master_locations_data()


