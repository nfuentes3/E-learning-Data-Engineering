import pandas as pd
from deltalake import write_deltalake, DeltaTable
import os


def crear_df(json, path=None):
    """Crea un DataFrame de Pandas, en formato JSON para ser usado en el DeltaLake.

    :param json: JSON a convertir en DataFrame
    :type json: dict
    :param path: Parámetro donde crear el DataFrame, por defecto no se indica (None)
    :type path: str, optional
    :return: Devuelve un DataFrame de Pandas.
    :rtype: DataFrame.pandas
    """
    try:
        df = pd.json_normalize(json, path)
        print(f"DataFrame creado con {len(df)} filas y {len(df.columns)} columnas")
        return df
    except Exception as err:
        print(f"No se pudo generar el DataFrame: {err}")


def crear_deltalake(datos, path):
    """Creacion de un DeltaLake mediante un DataFrame de pandas.

    :param datos: DataFrame para crear en un DeltaLake
    :type datos: DataFrame de Pandas
    :param path: Ruta donde se crea el archivo Parquet del DeltaLake
    :type path: str
    """
    try:
        df = pd.DataFrame(datos)
        if not os.path.exists(path):
            dl = write_deltalake(path, df, mode="ignore")
            print(f"DeltaLake creado en {path}")
            return dl
        else:
            dl = write_deltalake(path, df, mode="overwrite")
            print(f"DeltaLake actualizado en {path}")
            return dl
    except Exception as err:
        print(f"No se pudo crear el DeltaLake: {err}")


def actualizar_deltalake(nuevo_df, ruta_origen, ruta_destino):
    """Actualiza un DeltaLake tomando un archivo Parquet existente y un nuevo DataFrame.

    :param nuevo_df: Nuevo DataFrame para agregar al DeltaLake
    :type nuevo_df: DataFrame de Pandas
    :param ruta_origen: Ruta del archivo Parquet existente
    :type ruta_origen: str
    :param ruta_destino: Ruta donde se guardará el nuevo archivo Parquet
    :type ruta_destino: str
    """
    try:
        # Leer el archivo Parquet existente
        tabla_existente = DeltaTable(ruta_origen)
        df_existente = tabla_existente.to_pandas()

        # Concatenar el DataFrame existente con el nuevo
        df_actualizado = pd.concat([df_existente, nuevo_df], ignore_index=True)

        # Escribir el nuevo DataFrame en la ruta destino
        write_deltalake(ruta_destino, df_actualizado, mode="overwrite")
        print(f"DeltaLake actualizado en {ruta_destino}")
        return df_actualizado
    except Exception as err:
        print(f"No se pudo actualizar el DeltaLake: {err}")
