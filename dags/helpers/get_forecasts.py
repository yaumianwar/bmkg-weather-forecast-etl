import requests
import time
from helpers.db_connect import get_clickhouse_connection


def get_forecasts():
    
    # create clickhouse client
    client = get_clickhouse_connection()
    if client is None:
        return None
    

    # get location codes from master location
    parameters = {'adm3': '73.02.02'}
    result = client.query('select adm4 from master_locations where adm3 = %(adm3)s group by adm4', parameters=parameters)
    location_codes = [list(i)[0] for i in result.result_rows]

    final_forecasts_data = []
    
    # get forecasts data for each sub-district using location code (district/adm3)
    for location_code in location_codes:
        # make api request to get forecast data using location code (district/adm3)
        print("Fetching forecast data for location code: {}".format(location_code))
        response = requests.get("https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={}".format(location_code))

        try:
            response_data = response.json()['data']
        except:
            print("Error fetching data for location code: {}".format(location_code))
            continue

        # iterate response data. First iteration is for each sub-district
        for data in response_data:

            # get sub-district info
            location_data = {'location_code':data['lokasi']['adm4'], 'timezone':data['lokasi']['timezone']}
            forecasts = data['cuaca']

            # iterate forecast data
            for forecast in forecasts:

                # create unique forecast code using local_datetime and sub-district/adm4 code
                local_datetime = forecast[0]['local_datetime']
                forecast_code = "{}_{}".format(local_datetime, location_data['location_code'])
                location_data['forecast_code'] = forecast_code

                # concat forecast data and location info
                forecast_data = [{**location_data, **d} for d in forecast]

                # add forecast data
                final_forecasts_data.extend(forecast_data)
        
        time.sleep(2) 


    return final_forecasts_data

    