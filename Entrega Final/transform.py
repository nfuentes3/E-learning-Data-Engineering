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
                "last_updated",
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
                "wind_kph": "velocidad_viento_kph",
                "uv": "indice_uv",
                "ciudad": "nombre_ciudad",
                "condition.text": "condicion_climatica",
                "cloud": "nubosidad",
                "windchill_c": "sensacion_termica_viento",
                "dewpoint_c": "punto_rocio",
                "heatindex_c": "indice_calor",
                "wind_dir": "direccion_viento",
                "condition.text": "condicion_climatica",
                "last_updated": "ultima_actualizacion",
            },
            inplace=True,
        )
        return df
    except Exception as err:
        print(f"Error al normalizar el DataFrame: {err}")
        return df


def indicacion_pronostico(df):
    """Agrega una columna de indicaciones especiales segun los datos obtenidos del DataFrame.
    Se basa en la temperatura, sensacion termica, humedad, indice UV y nubosidad.

    :param df: DataFrame a agregar la columna de indicaciones
    :type df: DataFrame de Pandas
    :return: DataFrame con la columna de indicaciones agregada
    :rtype: DataFrame de Pandas
    """
    try:
        df = normalizar_df_clima(df)
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
            elif registro["indice_uv"] >= 6:
                indicaciones.append("Indice UV alto: protegerse del sol")
            elif registro["nubosidad"] >= 70 and registro["humedad"] >= 80:
                indicaciones.append("Posibles lluvias")
            elif (
                registro["rafaga_viento_kph"] >= 41
                and registro["elocidad_viento_kph"] >= 39
            ):
                indicaciones.append("Vientos fuertes. Precaucion al salir.")
            else:
                indicaciones.append("Sin indicaciones especiales")

        df["indicacion"] = indicaciones

        return df

    except Exception as err:
        print(f"Error al agregar la columna de indicaciones: {err}")
        return df
