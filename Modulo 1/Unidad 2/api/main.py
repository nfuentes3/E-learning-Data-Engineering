import requests
import pandas as pd
import pprint

URL = "https://api.currencybeacon.com/v1"
API_KEY = "0AUIQI1n3RgtwxAVZGgH4MBORIv1Kjjv"
endpoint = "currencies"

respuesta = requests.get(f"{URL}/{endpoint}?api_key={API_KEY}")

print(respuesta)

data_completa = respuesta.json()


monedas = data_completa["response"]
print(type(monedas))

for x in monedas:
    if x["name"] == "Argentine Peso":
        print(type(x))
        resultado = x.items()
        for y in resultado:
            print(y)
