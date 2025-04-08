# Segunda entrega intermedia - E-learning Data Engineering
_Nicolas Andres Fuentes - DNI 40.771.299_

Es una app que obtiene informacion de una API del clima, el cual se extrae informacion del clima de ciudades y se transforma para obtener datos limpios.

**La api utilizada es AccuWeather API: https://developer.accuweather.com/**

El programa realiza las siguientes acciones:

- Almacenamiento de datos tipo Full por medio de Overwrite de DeltaLake (Metadatos_ciudades)
- Almacenamiento de datos de tipo Incremental por medio de concat de Pandas (Clima_moron)
- Modificacion de columnas del DataFrame obtenido (normalizar_df_clima)
- Se realizaron 4 modificaciones de los datos obtenidos:
  - Conversion de datos de columnas
  - Renombrar columnas
  - Se creo una nueva columna logica basada en datos obtenidos (columna indicacion de funcion agregar_condicion_abrigo)
  - Cruzar dataframes utilizando MERGE (Agregando columna de indicacion a los metadatos de las ciudades)

---

## Importaciones e instalaciones necesarias

En los archivos del programa, se encuentra los modulos necesarios para su ejecución.
Para ejecutar correctamente el programa se debe instalar los siguientes requerimientos:

```bash
pip install -r requirements.txt
```

> [!IMPORTANT]
> Hay un archivo llamado config.py donde esta guardada la información sensible para el correcto funcionamiento de la aplicación.

## Modulos

El programa esta desarollado de forma modular y funcional, separado en 4 archivos de los cuales 3 son funcionalidades y uno solo "main.py" el cual se utiliza como archivo principal de ejecución.
A continuacion detallamos cada uno de los modulos:

## get_data.py

Este archivo contiene funciones para interactuar con la API de WeatherAPI y obtener información sobre metadatos de ciudades y detalles del clima actual. A continuación, se describe cada función y sus argumentos.

_Variables Globales_

- **`BASE_URL`**: URL base de la API de WeatherAPI.
- **`ENDPOINT_CLIMA`**: Endpoint indicado por la API el cual obtendra los datos del clima actuales.
- **`ENDPOINT_METADATOS`**: Endpoint indicado por la API el cual obtendra los metadatos de la ciudad indicada.

### Funciones

#### `obtener_metadatos(ciudad: str)`

Obtiene los metadatos de una ciudad específica, como país, estado, latitud y longitud.

- **Argumentos**:
  - `ciudad` (_str_): Nombre de la ciudad para la cual se desean obtener los metadatos.
- **Retorno**:
  - _dict_: Diccionario con los metadatos de la ciudad.

#### `obtener_clima(ciudad: str)`

Obtiene los datos del clima actual para la ciudad indicada en el argumento.

- **Argumentos**:
  - `ciudad` (_str_): Nombre de la ciudad para la cual se desean obtener el clima actual.
- **Retorno**:
  - _dict_: Diccionario con los datos del clima de la ciudad indicada.

#### `obtener_ultimo_parquet(ruta: str)`

Retorna el ultimo archivo .parquet generado, a fin de obtener los ultimos datos recibidos.

- **Argumentos**:
  - `ruta` (_str_): Ruta del DeltaLake donde se encuentran los archivos parquet.
- **Retorno**:
  - _str_: Nombre del ultimo archivo parquet generado.

## storage.py

Modulo que almacena los datos obtenidos de diferentes fuentes. Los mismos pueden ser guardados en formato DataFrame de Pandas como en formato DeltaLake

### Funciones

#### `crear_dataframe(json, path=None)`

Crea un DataFrame de Pandas a partir de un JSON, con la posibilidad de especificar un path para normalizar los datos.

- **Argumentos**:
  - `json` (_dict_): JSON que se desea convertir en un DataFrame.
  - `path` (_str, optional_): Parámetro opcional para especificar el path de normalización del JSON. Por defecto es `None`.
- **Retorno**:
  - _DataFrame_: DataFrame de Pandas generado a partir del JSON.

#### `guardar_deltalake(datos, path, mode="ignore")`

Crea un DeltaLake a partir de un DataFrame de Pandas y lo guarda en la ruta especificada.

- **Argumentos**:
  - `datos` (_dataframe_): Archivo DataFrame de pandas
  - `path` (_str, optional_): Ruta donde se crea el archivo Parquet del DeltaLake
  - `mode` (_str, ignore_): Modo de escritura en el DeltaLake. Por defecto es "ignore", pero puede ser tambien "append", "overwrite" o "error"
- **Retorno**:
  - _DeltaLake_: DeltaLake generado a partir de un DataFrame de pandas

#### `nuevo_registro_deltalake(nuevo_df, path)`

Genera un nuevo registro en el archivo .parquet que se encuentra en la ruta.
Lee los datos del ultimo archivo, y concatena con los nuevos datos recibidos.

- **Argumentos**:
  - `nuevo_df` (_dataframe_): Archivo DataFrame de pandas
  - `path` (_str, optional_): Ruta donde se crea el archivo Parquet del DeltaLake
- **Retorno**:
  - _DeltaLake_: DeltaLake generado a partir de un DataFrame de pandas

## transform.py

Es un modulo que realiza operaciones de modificacion y logicas sobre los DataFrames generados.
El mismo sirve para limpiar DataFrames con columnas que no sirven asi como como tambien agregar nuevas columnas con una logica de los datos obtenidos.

#### `nuevo_registro_deltalake(df)`

Normaliza un DataFrame eliminando columnas innecesarias, renombrando las columnas restantes y ajustando los tipos de datos a los más adecuados.

- **Argumentos**:
  - `df` (_dataframe_): Archivo DataFrame de pandas
- **Retorno**:
  - _DataFrame_: Retorna un Dataframe con limpieza de columnas, renombramiento de columnas y tipos de datos modificados.

#### `indicacion_pronostico(df)`

Agrega una columna de indicaciones especiales segun los datos obtenidos del DataFrame.
Se basa en la temperatura, sensacion termica, humedad, indice UV y nubosidad.

- **Argumentos**:
  - `df` (_dataframe_): Archivo DataFrame de pandas
- **Retorno**:
  - _DataFrame_: Retorna un Dataframe limpio con un agregado de columna que da indicaciones segun logicas obtenidas de datos meteorologicos.

## main.py

Este archivo es el punto de entrada principal del programa. Contiene la lógica para obtener, transformar y almacenar datos meteorológicos utilizando los módulos auxiliares `get_data.py`, `storage.py` y `transform.py`.

### Funciones

#### `pronostico_ciudades()`

Obtiene el clima de las ciudades guardadas en el DeltaLake y genera información más limpia con indicaciones lógicas.

- **Retorno**:
  - _Parquet de DeltaLake_: DeltaLake con el pronóstico del clima de las ciudades.

#### `registro_clima(ciudad: str)`

Obtiene los datos del clima de una ciudad específica desde Bronze y los guarda en el DeltaLake. Si se consulta nuevamente, genera un historial con fecha y hora de los resultados obtenidos.

- **Argumentos**:
  - `ciudad` (_str_): Nombre de la ciudad para la cual se desea registrar el clima.
- **Retorno**:
  - _DeltaLake_: DeltaLake con el clima registrado de la ciudad.

## Ejecución Principal

El bloque principal del archivo ejecuta las siguientes acciones:

1. **Definición de la lista de ciudades**: Se genera una lista de ciudades predeterminadas para extraer información.
2. **Generación de metadatos de ciudades**:
   - Se obtienen los metadatos de las ciudades de la lista y se almacenan en el DeltaLake en la ruta `data/accuweather_api/bronze/metadatos_ciudades`.
3. **Obtención de datos climáticos actuales**:
   - Se obtienen los datos climáticos de las ciudades de la lista y se almacenan en el DeltaLake en la ruta `data/accuweather_api/bronze/climas_ciudades`.
4. **Generación de pronósticos**:
   - Se genera un pronóstico del clima para las ciudades y se almacena en el DeltaLake en la ruta `data/accuweather_api/silver/pronostico_ciudades`.
5. **Registro de clima por ciudad**:
   - Se registra el clima de ciudades específicas (por ejemplo, "Tilcara", "San Miguel", "Oslo") y se almacena en el DeltaLake en rutas específicas.

## Ejemplo de Ejecución

```python
if __name__ == "__main__":
    print("Generando nuevos datos...")
    # Lista de ciudades
    lista_ciudades = [
        "Hurlingham", "Ushuaia", "La Falda", "Tilcara", "Bariloche", "Posadas",
        "Brasilia", "Detroit", "Paris", "Verona", "Berlin", "London", "Cairo",
        "Reykjavik", "Sydney", "Mumbai", "Cape Town", "Tokyo", "Anchorage",
        "Dubai", "Oslo",
    ]
    # Generación de metadatos
    metadatos_ciudades = [obtener_metadatos(ciudad) for ciudad in lista_ciudades]
    df_metadatos = crear_dataframe(metadatos_ciudades)
    guardar_deltalake(df_metadatos, f"{RUTA_BRONZE}/metadatos_ciudades")

    # Generación de datos climáticos
    climas_ciudades = [obtener_clima(ciudad) for ciudad in lista_ciudades]
    df_climas_ciudades = crear_dataframe(climas_ciudades)
    guardar_deltalake(df_climas_ciudades, f"{RUTA_BRONZE}/climas_ciudades")

    # Generación de pronósticos
    pronostico_ciudades()

    # Registro de clima por ciudad
    registro_clima("Tilcara")
    registro_clima("San Miguel") #El mismo dara error debido a que 'San Miguel' no se encuentra dentro de los DataLakes.
    registro_clima("Oslo")
```
