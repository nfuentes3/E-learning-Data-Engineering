import requests
from config import API_KEY
import os, glob

# La api a utilizar es la api del clima de AccuWeather: https://developer.accuweather.com/

BASE_URL = "http://api.weatherapi.com/v1"  # URL base de la API
ENDPOINT_CLIMA = "/current.json"  # Endpoint de clima actual
ENDPOINT_METADATOS = "/search.json"  # Endpoint de metadatos


def obtener_metadatos(ciudad: str):
    """Obtiene los metadatos de la ciudad a consultar (Pais, Estado, Latitud, Longitud)

    :param ciudad: Ciudad a consultar los metadatos
    :type ciudad: str
    :return: Diccionario con los metadatos de la ciudad
    :rtype: dict

    """
    url_endpoint = (
        f"{BASE_URL}{ENDPOINT_METADATOS}?key={API_KEY}&q={ciudad}&aqi=no&lang=es"
    )
    try:
        respuesta = requests.get(url_endpoint)
        respuesta = respuesta.json()[0]
        return respuesta
    except Exception as err:
        print(f"Error en la peticion: {err}")


def obtener_clima(ciudad: str):
    """Obtiene detalles del clima actual de la ciudad indicada.
    :param ciudad: Ciudad a consultar el clima actual
    :type ciudad: str
    :return: Diccionaro o JSON con los datos del clima
    :rtype: dict
    """
    url_endpoint = f"{BASE_URL}{ENDPOINT_CLIMA}?key={API_KEY}&q={ciudad}&aqi=no&lang=es"
    respuesta = requests.get(url_endpoint)
    respuesta = respuesta.json()
    data = respuesta["current"]
    data["ciudad"] = (
        ciudad  # Agregamos una columna con el nombre de la ciudad para identificar mejor a cual pertenece.
    )
    return data


def obtener_ultimo_parquet(ruta):
    """Obtiene el ultimo archivo .parquet generado a fin de obtener los datos mas recientes.

    :param ruta: Ruta del deltalake
    :type ruta: str
    :raises FileNotFoundError: Excecpcion en caso de no enctonrar archivos .parquet en la ruta indicada
    :return: Nombre del ultimo archivo .parquet generado.
    :rtype: str
    """
    try:
        archivos_parquet = glob.glob(os.path.join(ruta, "*.parquet"))
        if not archivos_parquet:
            raise FileNotFoundError(
                "No se encontraron archivos parquet en la ruta indicada."
            )
        ultimo_archivo = max(archivos_parquet, key=os.path.getmtime)
        return ultimo_archivo
    except Exception as err:
        print("No se pudo encontrar el ultimo archivo:", err)
