# Importamos las librerías necesarias
import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# Definimos las constantes para la API de REE
BASE_URL = "https://apidatos.ree.es/es/datos/"

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json"
}

ENDPOINTS = {
    "demanda": ("demanda/evolucion", "hour"),
    "balance": ("balance/balance-electrico", "day"),
    "generacion": ("generacion/potencia-instalada", "month"),
    "intercambios": ("intercambios/todas-fronteras-programados", "day")
}

# Función para consultar un endpoint, según los parámetros dados, de la API de REE 
# y devolver los datos en formato JSON
def get_data(endpoint_name, endpoint_info, params):
    path, time_trunc = endpoint_info
    params["time_trunc"] = time_trunc
    url = BASE_URL + path

    try:
        response = requests.get(url, headers=HEADERS, params=params)
        # Si la búsqueda no fue bien, se devuelve una lista vacía
        if response.status_code != 200:
            return []
        response_data = response.json()
    except Exception as e:
        return []

    data = []

    # Verificamos si el item tiene "content" y asumimos que es una estructura compleja
    for item in response_data.get("included", []):
        attrs = item.get("attributes", {})
        category = attrs.get("title")

        if "content" in attrs:
            for sub in attrs["content"]:
                sub_attrs = sub.get("attributes", {})
                sub_cat = sub_attrs.get("title")
                for entry in sub_attrs.get("values", []):
                    entry["primary_category"] = category
                    entry["sub_category"] = sub_cat
                    data.append(entry)
        else:
            # Procesamos las estructuras más simple (demanda, generacion), asumiendo que no hay subcategorías
            for entry in attrs.get("values", []):
                entry["primary_category"] = category
                entry["sub_category"] = None
                data.append(entry)

    return data

# Función para extraer los datos de los últimos x años
# Devolviendo un DataFrame de Pandas
def get_data_for_last_x_years(num_years=3):
    all_dfs = []
    current_date = datetime.now()
    # Calculamos el año de inicio a partir del año actual
    start_year_limit = current_date.year - num_years

    # Iteramos sobre cada año y mes
    for year in range(start_year_limit, current_date.year + 1):
        for month in range(1, 13):
            # Definimos el inicio de cada mes
            month_start = datetime(year, month, 1)

            # Si el inicio del mes está en el futuro, lo saltamos
            if month_start > current_date:
                continue

            # Calculamos el final del mes actual
            month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(minutes=1)

            # Ajustamos la fecha final por si es mayor que la fecha actual
            if month_end > current_date:
                end_date_for_request = current_date
            else:
                end_date_for_request = month_end

            # Iteramos sobre cada endpoint
            for name, (path, granularity) in ENDPOINTS.items():
                params = {
                    "start_date": month_start.strftime("%Y-%m-%dT%H:%M"),
                    "end_date": end_date_for_request.strftime("%Y-%m-%dT%H:%M"),
                    "geo_trunc": "electric_system",
                    "geo_limit": "peninsular",
                    "geo_ids": "8741"
                }

                month_data = get_data(name, (path, granularity), params)

                # Y sacamos los datos
                if month_data:
                    df = pd.DataFrame(month_data)
                    #Lidiamos con problemas de zona horaria en la columna "datetime"
                    try:
                        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                    except Exception as e:
                        continue

                    # Obtenemos nuevas columnas de año, mes, día, hora y endpoint
                    df['year'] = df['datetime'].dt.year
                    df['month'] = df['datetime'].dt.month
                    df['day'] = df['datetime'].dt.day
                    df['hour'] = df['datetime'].dt.strftime('%H:%M:%S')
                    df['endpoint'] = name

                    # Reordenamos las columnas
                    df = df[['value', 'percentage', 'datetime', 'primary_category',
                             'sub_category', 'year', 'month', 'day', 'hour', 'endpoint']]
                    all_dfs.append(df)

                time.sleep(1)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# Obtenemos los datos de los últimos 3 años a partir de hoy
ree_data_df = get_data_for_last_x_years(num_years=3)
ree_data_df
