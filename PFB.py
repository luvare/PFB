# Importamos las librerías necesarias
import requests
import pandas as pd
import json

# Definimos las constantes para la API de REE
BASE_URL = "https://apidatos.ree.es/es/datos/"

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "host": "apidatos.ree.es"
}

ENDPOINTS = {
    "demanda": "demanda/evolucion",
    "balance": "balance/balance-electrico",
    "generacion": "generacion/potencia-instalada",
    "intercambios": "intercambios/todas-fronteras-programados"
}

PARAMS = {
    "start_date": "2018-01-01T00:00",
    "end_date": "2018-12-31T23:59",
    "time_trunc": "month",
    "geo_trunc": "electric_system",
    "geo_limit": "peninsular",
    "geo_ids": "8741"
}

# Función para consultar un endpoint de la API de REE y devolver los datos en formato JSON
def get_data(endpoint_name, endpoint_path):
    url = BASE_URL + endpoint_path
    response = requests.get(url, headers=HEADERS, params=PARAMS)
    json_data = response.json()

    all_parsed_data = []

    for item in json_data.get("included", []):
        item_attributes = item.get("attributes", {})
        
        # Obtenemos el nombre de la categoría principal
        primary_category_name = item_attributes.get("title")

        # Verificamos si el item tiene "content" y asumimos que es una estructura compleja
        if "content" in item_attributes:
            for sub_item in item_attributes["content"]:
                sub_item_attributes = sub_item.get("attributes", {})
                sub_category_name = sub_item_attributes.get("title")
                values = sub_item_attributes.get("values", [])
                for entry in values:
                    entry["primary_category"] = primary_category_name
                    entry["sub_category"] = sub_category_name
                    all_parsed_data.append(entry)
        else:
            # Procesamos las estructuras más simple (demanda, generacion), asumiendo que no hay subcategorías
            values = item_attributes.get("values", [])
            for entry in values:
                entry["primary_category"] = primary_category_name
                entry["sub_category"] = None
                all_parsed_data.append(entry)

    return all_parsed_data

def main_consolidated_df():
    all_dfs = []

    for name, path in ENDPOINTS.items():
        raw_data_list = get_data(name, path)
        
        if raw_data_list:
            df_temp = pd.DataFrame(raw_data_list)
            all_dfs.append(df_temp)

    # Concatenamos todos los DataFrames en uno solo
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame() # Devuelve un DataFrame vacío si no hay datos


if __name__ == "__main__":
    # Llama a la función que devuelve el DataFrame consolidado
    ree_data_df = main_consolidated_df()

ree_data_df
