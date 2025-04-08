from get_data import obtener_clima, obtener_metadatos, obtener_ultimo_parquet
from storage import crear_dataframe, nuevo_registro_deltalake, guardar_deltalake
from transform import indicacion_pronostico
import datetime
import pandas as pd


# Definimos las rutas del DeltaLake
RUTA_BRONZE = "data/accuweather_api/bronze"
RUTA_SILVER = "data/accuweather_api/silver"


def pronostico_ciudades():
    """Obtiene el clima de las ciudades guardadas en el deltalake y obtiene informacion mas limpia con indicaciones logicas.

    :return: DeltaLake con clima de las ciudades.
    :rtype: Parquet de DeltaLake
    """
    try:
        # Establecemos la ruta objetivo donde se guardan los datos
        path = f"{RUTA_SILVER}/pronostico_ciudades"
        # Obtenemos los metadatos mas recientes
        metadatos = obtener_ultimo_parquet(
            "data/accuweather_api/bronze/metadatos_ciudades"
        )
        df_metadatos = pd.read_parquet(metadatos)
        # Seleccionamos únicamente las columnas necesarias
        df_metadatos = df_metadatos[["name", "region", "country"]]
        # Renombramos las columnas
        df_metadatos = df_metadatos.rename(
            columns={"name": "ciudad", "estado": "pais", "country": "pais"}
        )
        # Obtenemos los datos del clima mas recientes (temperatura y condicion)
        clima = obtener_ultimo_parquet("data/accuweather_api/bronze/climas_ciudades")
        df_clima = pd.read_parquet(clima)
        # Agregamos columna de indicaciones
        df_clima = indicacion_pronostico(df_clima)
        # Seleccionamos las columnas necesarias
        df_clima = df_clima[
            ["temperatura", "humedad", "condicion_climatica", "indicacion"]
        ]
        # Unificamos ambos dataframe con la operacion merge y añadimos la columna con la fecha de consulta
        df_final = pd.merge(
            df_metadatos.reset_index(drop=True),
            df_clima.reset_index(drop=True),
            left_index=True,
            right_index=True,
        )
        df_final["fecha_consulta"] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")
        dl_final = guardar_deltalake(df_final, path)
        return dl_final
    except Exception as err:
        print("No se pudo obtener la informacion solicitada:", err)


def registro_clima(ciudad: str):
    """Obtiene los datos del clima de una ciudad desde Bronze y los guarda en el DeltaLake en su ubicacion.
    En caso de volver a consutlar, generara un historial con fecha y hora de los resultados obtenidos.

    :param ciudad: Ciudad a registrar clima
    :type ciudad: str
    :return: DeltaLake del clima obtenido
    :rtype: DeltaLake
    """
    try:
        # Definimos datos de guardado
        path = f"{RUTA_SILVER}/clima/{ciudad}"
        # Obtenemos metadatos
        df_metadatos = pd.read_parquet("data/accuweather_api/bronze/metadatos_ciudades")
        # Filtramos los datos coincidentes con la ciudad
        df_ciudad = df_metadatos[df_metadatos["name"] == ciudad]
        if not df_ciudad.empty:
            # Seleccionamos solamente las columnas que queremos
            df_metadatos = df_ciudad[["name", "region", "country"]]
            # Obtenemos clima
            df_clima = pd.read_parquet("data/accuweather_api/bronze/climas_ciudades")
            df_clima = df_clima[df_clima["ciudad"] == ciudad]
            # Seleccionamos las columnas que solamente queremos
            df_clima = df_clima[
                ["temp_c", "feelslike_c", "humidity", "cloud", "condition.text"]
            ]
            # Unificamos dataframes y agregamos columna de registro de fecha
            df_final = pd.merge(
                df_metadatos.reset_index(drop=True),
                df_clima.reset_index(drop=True),
                left_index=True,
                right_index=True,
            )
            df_final["seed"] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            # Lo guardamos en una ubicacion aparte
            dl_final = nuevo_registro_deltalake(df_final, path)
            return dl_final
        else:
            print(f"{ciudad}: No se pudo encontrar la ciudad en los datos guardados")
    except Exception as err:
        print("No se pudo obtener el clima:", err)


if __name__ == "__main__":
    print("Generando nuevos datos...")
    # Se genera una lista de ciudades determinadas para realizar la extraccion de la informacion.
    lista_ciudades = [
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
        "Cairo",
        "Reykjavik",
        "Sydney",
        "Mumbai",
        "Cape Town",
        "Tokyo",
        "Anchorage",
        "Dubai",
        "Oslo",
    ]
    ## Generamos los metadatos de las ciudades de la lista para guardarlas en el DeltaLake
    metadatos_ciudades = []
    for ciudad in lista_ciudades:
        resultado = obtener_metadatos(ciudad)
        metadatos_ciudades.append(resultado)
    df_metadatos = crear_dataframe(metadatos_ciudades)
    path_metadatos = f"{RUTA_BRONZE}/metadatos_ciudades"
    dl_metadatos = guardar_deltalake(df_metadatos, path_metadatos)

    ## Generamos los datos del clima actuales de la lista y lo guardamos en el DeltaLake
    climas_ciudades = []
    for ciudad in lista_ciudades:
        resultado = obtener_clima(ciudad)
        climas_ciudades.append(resultado)
    df_climas_ciudades = crear_dataframe(climas_ciudades)
    path_climas = f"{RUTA_BRONZE}/climas_ciudades"
    dl_climas_ciudades = guardar_deltalake(df_climas_ciudades, path_climas)
    ## Obtenemos los datos del clima de las ciudades y generamos un pronostico para guardarlo en DeltaLake
    nuevo_pronostico = pronostico_ciudades()
    ## Generamos clima segun ciudad de los datos obtenidos anteriormente y generamos un nuevo deltalake con un registro de log.
    clima_tilcara = registro_clima("Tilcara")
    clima_sanmi = registro_clima(
        "San Miguel"
    )  # En caso de consultar por una ciudad que no se encuentra en la instancia Bronze, la misma indicara un error.
    clima_oslo = registro_clima("Oslo")
