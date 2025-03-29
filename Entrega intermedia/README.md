# Segunda entrega intermedia - E-learning Data Engineering

## Nicolas Andres Fuentes - DNI 40.771.299

Es una app que obtiene informacion de una API del clima, el cual se extrae informacion del clima de ciudades y se transforma para obtener datos limpios.

La api utilizada es AccuWeather API: https://developer.accuweather.com/
Cuenta con API KEY el cual esta declarada dentro del programa.

El programa realiza las siguientes acciones:

- Almacenamiento de datos tipo Full por medio de Overwrite de DeltaLake (Metadatos_ciudades)
- Almacenamiento de datos de tipo Incremental por medio de concat de Pandas (Clima_moron)
- Modificacion de columnas del DataFrame obtenido (normalizar_df_clima)
- Se realizaron 4 modificaciones de los datos obtenidos:
  - Conversion de datos de columnas
  - Renombrar columnas
  - Se creo una nueva columna logica basada en datos obtenidos (columna indicacion de funcion agregar_condicion_abrigo)
  - Cruzar dataframes utilizando MERGE (Agregando columna de indicacion a los metadatos de las ciudades)

### Importaciones e instalaciones necesarias

#### Instalaciones

Para ejecutar correctamente el programa se debe instalar los siguientes requerimientos:

```bash
pip install -r requirements.txt
```

# Modulos

El programa esta desarollado de forma modular y funcional, separado en 4 archivos de los cuales 3 son funcionalidades y uno solo "main.py" el cual se utiliza como archivo principal de ejecución.
A continuacion detallamos cada uno de los modulos:

## get_data.py

Este archivo contiene funciones para interactuar con la API de WeatherAPI y obtener información sobre metadatos de ciudades y detalles del clima actual. A continuación, se describe cada función y sus argumentos.

---

## Variables Globales

- **`BASE_URL`**: URL base de la API de WeatherAPI.
- **`API_KEY`**: Clave de acceso para autenticar las solicitudes a la API.

---

## Funciones

### `obtener_metadatos(ciudad: str)`

Obtiene los metadatos de una ciudad específica, como país, estado, latitud y longitud.

- **Argumentos**:
  - `ciudad` (_str_): Nombre de la ciudad para la cual se desean obtener los metadatos.
- **Retorno**:
  - _dict_: Diccionario con los metadatos de la ciudad.

### `guardar_metadatos(lista_ciudades: list)`

Obtiene los metadatos de una lista de ciudades indicada, itera en cada una para obtener la informacion de cada uno de los elementos de la lista.

- **Argumentos**:
  - `lista_ciudades` (_list_): Lista de ciudades a obtener los metadatos
- **Retorno**:
  - _dict_: Diccionario con los metadatos de las ciudades.

### `obtener_clima(ciudad: str)`

Obtiene los detalles del clima actual de una ciudad específica utilizando la API de WeatherAPI.

#### Argumentos

- **`ciudad`** (_str_): Nombre de la ciudad para la cual se desea consultar el clima actual.

#### Retorno

- **`dict`**: Diccionario con los datos del clima actual. Incluye información como:
  - `temp_c`: Temperatura actual en grados Celsius.
  - `humidity`: Porcentaje de humedad.
  - `wind_kph`: Velocidad del viento en kilómetros por hora.
  - `condition`: Condición climática (ejemplo: Soleado, Nublado, etc.).
  - `ciudad`: Nombre de la ciudad consultada.

### `obtener_climas_todos(lista_ciudades: list)`

Obtiene el clima actual de todas las ciudades indicadas en una lista utilizando la API de WeatherAPI.

#### Argumentos

- **`lista_ciudades`** (_list_): Lista de nombres de ciudades para las cuales se desea consultar el clima actual.

#### Retorno

- **`list`**: Lista de diccionarios, donde cada diccionario contiene los datos del clima actual de una ciudad. Cada diccionario incluye:
  - `temp_c`: Temperatura actual en grados Celsius.
  - `humidity`: Porcentaje de humedad.
  - `wind_kph`: Velocidad del viento en kilómetros por hora.
  - `condition`: Condición climática (ejemplo: Soleado, Nublado, etc.).
  - `ciudad`: Nombre de la ciudad consultada.

## storage.py

Modulo que almacena los datos obtenidos de diferentes fuentes. Los mismos pueden ser guardados en formato DataFrame de Pandas como en formato DeltaLake

## Funciones

### `crear_df(json, path=None)`

Crea un DataFrame de Pandas a partir de un JSON, con la posibilidad de especificar un path para normalizar los datos.

#### Argumentos

- **`json`** (_dict_): JSON que se desea convertir en un DataFrame.
- **`path`** (_str, optional_): Parámetro opcional para especificar el path de normalización del JSON. Por defecto es `None`.

#### Retorno

- **`DataFrame`**: DataFrame de Pandas generado a partir del JSON.

### `crear_deltalake(datos, path)`

Crea un DeltaLake a partir de un DataFrame de Pandas y lo guarda en la ruta especificada.

#### Argumentos

- **`datos`** (_DataFrame_): DataFrame de Pandas que se desea guardar en el DeltaLake.
- **`path`** (_str_): Ruta donde se creará el archivo Parquet del DeltaLake.

#### Retorno

- **`DeltaLake`**: Objeto DeltaLake creado o actualizado.

### `actualizar_deltalake(nuevo_df, ruta_origen, ruta_destino)`

Actualiza un DeltaLake existente combinando un archivo Parquet con un nuevo DataFrame y guardando el resultado en una nueva ruta.

#### Argumentos

- **`nuevo_df`** (_DataFrame_): Nuevo DataFrame que se desea agregar al DeltaLake.
- **`ruta_origen`** (_str_): Ruta del archivo Parquet existente que se desea actualizar.
- **`ruta_destino`** (_str_): Ruta donde se guardará el nuevo archivo Parquet actualizado.

#### Retorno

- **`DataFrame`**: DataFrame actualizado que combina los datos existentes con los nuevos.

## transform.py

Es un modulo que realiza operaciones de modificacion y logicas sobre los DataFrames generados.
El mismo sirve para limpiar DataFrames con columnas que no sirven asi como como tambien agregar nuevas columnas con una logica de los datos obtenidos.

### `normalizar_df_clima(df)`

Normaliza un DataFrame eliminando columnas innecesarias, renombrando las columnas restantes y ajustando los tipos de datos a los más adecuados.

#### Argumentos

- **`df`** (_DataFrame_): DataFrame de Pandas que se desea normalizar.

#### Retorno

- **`DataFrame`**: DataFrame normalizado con las columnas renombradas, tipos de datos ajustados y sin columnas innecesarias.

### `agregar_condicion_abrigo(df)`

Agrega una columna al DataFrame con indicaciones especiales basadas en las condiciones climáticas, como temperatura, sensación térmica, humedad, índice UV y nubosidad.

#### Argumentos

- **`df`** (_DataFrame_): DataFrame de Pandas al que se desea agregar la columna de indicaciones.

#### Retorno

- **`DataFrame`**: DataFrame con una nueva columna llamada `indicacion`, que contiene mensajes personalizados según las condiciones climáticas.

## main.py

Es el archivo principal de ejecucion, donde se importan los demas modulos y se realizan acciones para realizar acciones de:

- Obtencion de datos
- Almacenamiento de datos en formato DataFrame y DeltaLake
- Transformacion de los datos, segun metodos declarados en los modulos.
