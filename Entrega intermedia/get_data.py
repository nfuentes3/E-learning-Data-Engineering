import requests
import datetime

# La api a utilizar es la api del clima de AccuWeather: https://developer.accuweather.com/

BASE_URL = "http://api.weatherapi.com/v1"  # URL base de la API

API_KEY = "02b563c1944f4676b24200206251803"  # Api key

# Genero una lista de ciudades a consultar
ciudades = [
    "Hurlingham",
    "Ushuaia",
    "La Falda",
    "Tilcara",
    "Bariloche",
    "Posadas",
    "Brasilia",
    "Detroit",
    "Paris",
    "Verona",
    "Berlin",
    "London",
]


def obtener_metadatos(ciudad: str):
    """Obtiene los metadatos de la ciudad a consultar (Pais, Estado, Latitud, Longitud)

    :param ciudad: Ciudad a consultar los metadatos
    :type ciudad: str
    :return: Diccionario con los metadatos de la ciudad
    :rtype: dict

    """
    endpoint = "/search.json"  # Endpoint de los metadatos
    url_endpoint = f"{BASE_URL}{endpoint}?key={API_KEY}&q={ciudad}&aqi=no&lang=es"
    try:
        respuesta = requests.get(url_endpoint)
        respuesta = respuesta.json()[0]
        data = {  # Genero un diccionario con los metadatos necesarios
            "ciudad": respuesta["name"],
            "pais": respuesta["country"],
            "estado": respuesta["region"],
            "latitud": respuesta["lat"],
            "longitud": respuesta["lon"],
        }
        return data
    except Exception as err:
        print(f"Error en la peticion: {err}")


def guardar_metadatos(lista_ciudades: list):
    """Obtiene todos los metadatos de las ciudades indicadas.
    Espera una lista de ciudades para iterar por cada una, y guardar los datos en una lista.

    :return: Todos los metadatos de una lista de ciudades
    :rtype: list
    """
    metadatos = []
    for x in lista_ciudades:
        registro = obtener_metadatos(x)
        metadatos.append(registro)
    return metadatos


def obtener_clima(ciudad):
    """Obtiene detalles del clima actual de la ciudad indicada.

    :param ciudad: Ciudad a consultar el clima actual
    :type ciudad: str
    :return: Diccionaro o JSON con los datos del clima
    :rtype: dict
    """
    endpoint = "/current.json"
    url_endpoint = f"{BASE_URL}{endpoint}?key={API_KEY}&q={ciudad}&aqi=no&lang=es"
    respuesta = requests.get(url_endpoint)
    respuesta = respuesta.json()
    data = {
        "ciudad": respuesta["location"]["name"],
        "pais": respuesta["location"]["country"],
        "temperatura": respuesta["current"]["temp_c"],
        "sensasion_termica": respuesta["current"]["feelslike_c"],
        "humedad": respuesta["current"]["humidity"],
        "condicion": respuesta["current"]["condition"]["text"],
        "fecha_lectura": datetime.datetime.now().strftime("%d-%m-%Y %H:%M"),
    }
    return data
