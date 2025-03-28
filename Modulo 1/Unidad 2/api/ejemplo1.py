import requests
from pprint import pprint

"""url = "https://api.github.com"

r = requests.get(url)

print(type(r))
dir(r)

print(f"codigo: {r.status_code}")

data = r.json()

print(type(data))

campo = "code_search_url"
print(data[campo])

print(data.keys())"""

r = requests.get(
    url="https://api.github.com/search/repositories",
    params={
        "q": "java",
    },
)

if r.status_code == 200:
    print(f"La peticion fue exitosa, se obtuvo una respuesta de tipo: {type(r.json())}")
else:
    print(f"Error en la peticion: {r.status_code, r.content}")
