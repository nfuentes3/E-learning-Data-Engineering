*Para visualizar este archivo de una forma mas amigable, en el VSCode, presionar la combinación de teclas `Ctrl+Shift+P`, escribir y seleccionar Markdown: Open preview*

## Archivo de configuración
Es un archivo de texto, la extensión puede ser `.conf`, `.ini`, `.cfg`, etc. En este caso, vamos a almacenar credenciales de acceso como de la base de datos, para no exponerlas en el código fuente ya que representa un riesgo de seguridad.

Este archivo contiene secciones, delimintadas por corchetes `[]`, y cada sección contiene pares de clave-valor, separados por un signo de igualdad `=`, como 
si fuese que estamos asignando un valor a una variable en Python.
Una sección puede contener datos de conexión a una base de datos, por ejemplo, y otra sección puede contener datos de conexión a una API.

En Python vamos a utilizar la librería `configparser` para leerlos.

## Driver de base de datos
Como vamos a interactuar con diferentes motores de base de datos, vamos a necesitar un driver para cada uno de ellos. Un driver es un conjunto de librerías que nos permite interactuar con un motor de base de datos específico. Por ejemplo, para interactuar con MySQL necesitamos el driver de MySQL, para interactuar con PostgreSQL necesitamos el driver de PostgreSQL, y así sucesivamente.

En el caso de Python, primero vamos a utilizar la librería SQLAlchemy, ya que nos permite interactuar con diferentes motores de base de datos a través de una interfaz común.

A su vez, SQLAlchemy necesita un driver específico para cada motor de base de datos con el que vamos a interactuar, en definitiva es una librería de Python adicional. Es decir que además de instalar SQLAlchemy, hay que instalar el driver de la base con la que vamos a interactuar. En la siguiente tabla, podes ver la librería a instalar según la base de datos, junto con el nombre del driver que hay que indicar al momento de establecer la conexión. 

| Motor de base de datos | Librería de Python | Nombre del driver   |
|------------------------|--------------------|---------------------|
| MySQL                  | pymysql            | mysql+pymysql       |
| PostgreSQL             | psycopg2           | postgresql+psycopg2 |
| SQL Server             | pymssql            | mssql+pymssql       |
| Oracle                 | cx_oracle          | oracle+cx_oracle    |