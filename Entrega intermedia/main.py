from get_data import guardar_metadatos, obtener_clima, obtener_climas_todos
from storage import crear_df, crear_deltalake, actualizar_deltalake
from transform import normalizar_df_clima, agregar_condicion_abrigo
import pandas as pd
import os

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
    "Roma",
]

if __name__ == "__main__":
    # Obtenemos el clima para todas las ciudades
    climas = obtener_climas_todos(lista_ciudades)
    # Creamos un DataFrame con los datos obtenidos
    df = crear_df(climas)
    # Ajustamos las columnas y los tipos de datos con el metodo normalizar_df_clima
    df = normalizar_df_clima(df)
    # Agregamos la columna de indicaciones con el metodo agregar_condicion_abrigo
    df = agregar_condicion_abrigo(df)
    # Guardamos el DataFrame en un DeltaLake
    crear_deltalake(df, "data/clima_ciudades")
    # Generamoos los metadatos de las ciudades
    metadatos = guardar_metadatos(lista_ciudades)
    # Creamos un DataFrame con los metadatos
    df_metadatos = crear_df(metadatos)
    # Agregamos al DataFrame de metadatos la columna de indicaciones usando MERGE
    df_metadatos = pd.merge(
        df_metadatos,
        df[["nombre_ciudad", "indicacion"]],
        left_on="name",
        right_on="nombre_ciudad",
        how="left",
    )
    # Guardamos el DataFrame de metadatos en un DeltaLake
    dl_metadatos = crear_deltalake(df_metadatos, "data/metadatos_ciudades")
    # Generamos un nuevo df para consultar sobre el clima y generar datos incrementales
    clima_moron = obtener_clima("Moron")
    df_moron = crear_df(clima_moron)
    # Creamos DeltaLake
    dl_moron = crear_deltalake(df_moron, "data/clima_moron")
    # Hacemos una nueva consulta y agregamos el nuevo registro al DeltaLake creado
    nuevo_clima_moron = obtener_clima("Moron")
    nuevo_df_moron = crear_df(nuevo_clima_moron)
    # Actualizamos el DeltaLake con el nuevo registro
    nuevo_dl_moron = actualizar_deltalake(
        nuevo_df_moron, "data/clima_moron", "data/clima_moron"
    )
