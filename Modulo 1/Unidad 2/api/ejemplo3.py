import requests
import pandas as pd
import pprint

url = "https://api.disneyapi.dev/character"
endpoint = "character"

endpoint_url = f"{url}/{endpoint}"


respuesta = requests.get(endpoint_url)
formateado = respuesta.json()
print(type(formateado))

data = formateado["data"]
for pj in data:
    print(pj["name"])
