def normalizar_df_clima(df):
    """Elimina las columnas innecesarias y renombra las columnas restantes.
    Cambia los tipos de datos de las columnas a los tipos más adecuados.

    :param df: DataFrame a normalizar
    :type df: DataFrame de Pandas
    :return: DataFrame normalizado
    :rtype: DataFrame de Pandas
    """
    try:
        df = df.drop(
            columns=[
                "last_updated_epoch",
                "temp_f",
                "is_day",
                "wind_degree",
                "feelslike_f",
                "windchill_f",
                "heatindex_f",
                "dewpoint_f",
                "condition.icon",
                "condition.code",
                "vis_miles",
                "gust_mph",
                "wind_mph",
                "pressure_in",
                "precip_in",
            ]
        )
        df["humidity"] = df["humidity"].astype("int8")
        df["pressure_mb"] = df["pressure_mb"].astype("int16")
        df["wind_dir"] = df["wind_dir"].astype("category")
        df.reset_index(drop=True, inplace=True)
        col_nombre = df.pop("ciudad")
        df.insert(0, "ciudad", col_nombre)

        df.rename(
            columns={
                "temp_c": "temperatura",
                "feelslike_c": "sensacion_termica",
                "wind_kph": "viento_kph",
                "pressure_mb": "presion_mb",
                "precip_mm": "precipitacion_mm",
                "humidity": "humedad",
                "cloud": "nubosidad",
                "vis_km": "visibilidad_km",
                "gust_kph": "rafaga_viento_kph",
                "uv": "indice_uv",
                "ciudad": "nombre_ciudad",
                "last_updated": "ultima_actualizacion",
                "condition.text": "condicion_climatica",
                "cloud": "nubosidad",
                "windchill_c": "sensacion_termica_viento",
                "dewpoint_c": "punto_rocio",
                "heatindex_c": "indice_calor",
                "gust_kph": "rafaga_viento_kph",
                "wind_dir": "direccion_viento",
                "gust_kph": "rafaga_viento_kph",
                "condition.text": "condicion_climatica",
                "last_updated": "ultima_actualizacion",
            },
            inplace=True,
        )
        return df
    except Exception as err:
        print(f"Error al normalizar el DataFrame: {err}")
        return df


def agregar_condicion_abrigo(df):
    """Agrega una columna de indicaciones especiales segun los datos obtenidos del DataFrame.
    Se basa en la temperatura, sensacion termica, humedad, indice UV y nubosidad.

    :param df: DataFrame a agregar la columna de indicaciones
    :type df: DataFrame de Pandas
    :return: DataFrame con la columna de indicaciones agregada
    :rtype: DataFrame de Pandas
    """
    try:
        indicaciones = []

        for _, registro in df.iterrows():
            if (
                registro["temperatura"] > 30
                and registro["sensacion_termica"] > 32
                and registro["humedad"] >= 50
            ):
                indicaciones.append("Hace calor: hidratarse y usar ropa ligera")
            elif registro["temperatura"] < 10 and registro["sensacion_termica"] < 10:
                indicaciones.append("Hace frío: usar abrigo y evitar salir")
            elif (
                15 < registro["temperatura"] < 25
                and 15 < registro["sensacion_termica"] < 25
                and 40 < registro["humedad"] < 50
            ):
                indicaciones.append("Clima agradable")
            elif registro["indice_uv"] >= 6:
                indicaciones.append("Indice UV alto: usar protector solar")
            elif registro["nubosidad"] >= 80:
                indicaciones.append("Posibles lluvias")
            else:
                indicaciones.append("Sin indicaciones especiales")

        df["indicacion"] = indicaciones

        return df

    except Exception as err:
        print(f"Error al agregar la columna de indicaciones: {err}")
        return df
