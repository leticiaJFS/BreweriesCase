import requests
import json
import os

# Caminho absoluto da pasta Bronze
BRONZE_PATH = os.path.join(os.getcwd(), "data", "bronze")
FILENAME = "breweries_raw.json"

def fetch_breweries():
    all_data = []
    per_page = 50
    page = 1

    print("Buscando dados da API...")

    while True:
        url = f"https://api.openbrewerydb.org/v1/breweries?per_page={per_page}&page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar API: {response.status_code}")

        data = response.json()
        if not data:  # Se não houver mais dados, para o loop
            break

        all_data.extend(data)
        print(f"Página {page} baixada, total acumulado: {len(all_data)}")
        page += 1

    print(f"Total de registros obtidos: {len(all_data)}")
    return all_data

def save_bronze(data):
    os.makedirs(BRONZE_PATH, exist_ok=True)
    filepath = os.path.join(BRONZE_PATH, FILENAME)
    print(f"Tentando salvar em: {filepath}")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"Dados salvos com sucesso em {filepath}")

if __name__ == "__main__":
    data = fetch_breweries()
    save_bronze(data)
