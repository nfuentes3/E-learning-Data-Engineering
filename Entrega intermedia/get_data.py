import requests

# La api a utilizar es la api del clima de AccuWeather: https://developer.accuweather.com/

BASE_URL = "http://api.weatherapi.com/v1"  # URL base de la API

API_KEY = "02b563c1944f4676b24200206251803"  # Api key


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
        return respuesta
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
    data = respuesta["current"]
    data["ciudad"] = ciudad
    return data


def obtener_climas_todos(lista_ciudades: list):
    """Obtiene el clima actual de todas las ciudades indicadas en la lista.

    :param lista_ciudades: Lista de ciudades a consultar el clima actual
    :type lista_ciudades: list
    :return: Lista de diccionarios con los datos del clima de cada ciudad
    :rtype: list
    """
    climas = []
    for ciudad in lista_ciudades:
        clima = obtener_clima(ciudad)
        climas.append(clima)
    return climas
