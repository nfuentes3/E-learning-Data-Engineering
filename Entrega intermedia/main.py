from get_data import guardar_metadatos, obtener_clima
from storage import crear_df, crear_deltalake, actualizar_deltalake
import pprint, os

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
]


def actualizar_clima(ciudad, ruta):
    if not os.path.exists(ruta):
        clima = obtener_clima(ciudad)
        df = crear_df(clima)
        crear_deltalake(df, ruta)
        print(f"DeltaLake creado en {ruta}")
    else:
        clima = obtener_clima(ciudad)
        df = crear_df(clima)
        actualizar_deltalake(df, ruta, ruta)
        print(f"DeltaLake actualizado en {ruta}")


if __name__ == "__main__":
    clima_tilcara = actualizar_clima("Tilcara", "data/clima_tilcara")
    """metadatos_ciudades = guardar_metadatos(lista_ciudades)
    pprint.pprint(metadatos_ciudades)
    dl_metadatos = crear_deltalake(metadatos_ciudades, "data/metadatos_ciudades")
    print("DeltaLake creado con los metadatos de las ciudades.")"""
