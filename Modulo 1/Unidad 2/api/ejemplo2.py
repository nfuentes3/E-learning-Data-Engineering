import requests
import pandas as pd
import pprint
from datetime import datetime, timedelta


def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()

        try:
            data = response.json()
            if data_field:
                data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        print(f"La peticion ha fallado. Codigo de error: {e}")
        return None


def build_table(json_data):
    try:
        df = pd.json_normalize(json_data)
        return df
    except:
        print("Los datos no estan en el formato esperado")
        return None


url = "https://api.luchtmeetnet.nl/open_api"

"""endpoint = "stations"

json_data = get_data(url, endpoint, data_field="data")

pprint.pprint(json_data)

print(type(json_data))
print(type(json_data[10]))

endpoint = "components"

json_data = get_data(url, endpoint, data_field="data")
df = build_table(json_data)

pprint.pprint(df)
pprint.pprint(df.head())"""
#############################
endpoint = "stations"

params = {"organistation_id": "1"}

stations = get_data(url, endpoint, "data", params=params)
if stations:
    df_stations = build_table(stations)
    print(df_stations)
    print(df_stations.head())
#############################

all_stations = []

for station in stations:
    endpoint = f"stations/{station['number']}"

    station_details = get_data(url, endpoint, data_field="data")
    if station_details:
        station_details["number"] = station["number"]
        all_stations.append(station_details)

print(all_stations)

df_stations = build_table(all_stations)
print(df_stations)
print(df_stations.head())
