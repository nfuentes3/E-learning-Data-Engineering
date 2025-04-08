import pandas as pd
from deltalake import write_deltalake, DeltaTable
import os


def crear_dataframe(json, path=None):
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


def guardar_deltalake(datos, path, mode="ignore"):
    """Creacion de un DeltaLake mediante un DataFrame de pandas.

    :param datos: DataFrame para crear en un DeltaLake
    :type datos: DataFrame de Pandas
    :param path: Ruta donde se crea el archivo Parquet del DeltaLake
    :type path: str
    """
    try:
        df = pd.DataFrame(datos)
        if not os.path.exists(path):
            dl = write_deltalake(path, df, mode=mode)
            print(f"Nuevos datos guardados en DeltaLake: {path}")
            return dl
        else:
            dl = write_deltalake(path, df, mode="overwrite")
            print(f"DeltaLake actualizado en {path}")
            return dl
    except Exception as err:
        print(f"No se pudo crear el DeltaLake: {err}")


def nuevo_registro_deltalake(nuevo_df, path):
    """Genera un nuevo registro en el archivo .parquet que se encuentra en la ruta.
    Lee los datos del ultimo archivo, y concatena con los nuevos datos recibidos.

    :param path: _description_
    :type path: _type_
    :param nuevo_df: _description_
    :type nuevo_df: _type_
    :return: _description_
    :rtype: _type_
    """
    try:
        if os.path.exists(path):
            dt = DeltaTable(path)
            df_actual = dt.to_pandas()
            nuevo_dl = pd.concat([df_actual, nuevo_df], ignore_index=True)
            add = guardar_deltalake(nuevo_dl, path, mode="ignore")
            print(f"Se han agregado un nuevo registro en {path}")
            return add
        else:
            guardar_deltalake(nuevo_df, path)
    except Exception as err:
        print("No se pudo actualizar:", err)
