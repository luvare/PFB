!pip install supabase pandas requests
import pandas as pd
import requests
import time
import uuid
from datetime import datetime, timedelta, timezone
from supabase import create_client

# 🔐 Configura tus claves de Supabase
SUPABASE_URL = "https://gbfxqkzjzamqlqhzvbqc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdiZnhxa3pqemFtcWxxaHp2YnFjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkwMjc3MDksImV4cCI6MjA2NDYwMzcwOX0.ju_muEo9aTGT8FWFYpP-5_uEaywdSn7xOPllt1VQtUQ"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://apidatos.ree.es/es/datos/"
HEADERS = {"accept": "application/json", "content-type": "application/json"}

# Endpoints disponibles
ENDPOINTS = {
    "demanda": ("demanda/evolucion", "hour"),
    "balance": ("balance/balance-electrico", "day"),
    "generacion": ("generacion/evolucion-renovable-no-renovable", "day"),
    "intercambios": ("intercambios/todas-fronteras-programados", "day"),
    "intercambios_baleares": ("intercambios/enlace-baleares", "day"),
}

def get_data(endpoint_name, endpoint_info, params):
    path, time_trunc = endpoint_info
    params["time_trunc"] = time_trunc
    url = BASE_URL + path
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            return []
        return response.json()
    except:
        return []

def extraer_datos_ultimas_12h():
    dataframes = {}
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=12)

    for name, (path, granularity) in ENDPOINTS.items():
        params = {
            "start_date": start.strftime("%Y-%m-%dT%H:%M"),
            "end_date": now.strftime("%Y-%m-%dT%H:%M"),
            "geo_trunc": "electric_system",
            "geo_limit": "peninsular",
            "geo_ids": "8741"
        }

        response = get_data(name, (path, granularity), params)
        all_data = []

        for item in response.get("included", []):
            attrs = item.get("attributes", {})
            title = attrs.get("title")

            if "content" in attrs:
                for sub in attrs["content"]:
                    sub_attrs = sub.get("attributes", {})
                    sub_cat = sub_attrs.get("title")
                    for val in sub_attrs.get("values", []):
                        val["primary_category"] = title
                        val["sub_category"] = sub_cat
                        all_data.append(val)
            else:
                for val in attrs.get("values", []):
                    val["primary_category"] = title
                    val["sub_category"] = None
                    all_data.append(val)

        if all_data:
            df = pd.DataFrame(all_data)
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df["year"] = df["datetime"].dt.year
            df["month"] = df["datetime"].dt.month
            df["day"] = df["datetime"].dt.day
            df["hour"] = df["datetime"].dt.hour
            df["extraction_timestamp"] = datetime.now(timezone.utc)
            df["record_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

            columnas_base = ['record_id', 'value', 'percentage', 'datetime',
                             'primary_category', 'year', 'month', 'day',
                             'hour', 'extraction_timestamp']

            if name not in ['demanda', 'generacion', 'intercambios_baleares']:
                columnas_base.insert(5, 'sub_category')  # Agrega sub_category solo si aplica

            df = df[columnas_base]
            dataframes[name] = df

    return dataframes

def subir_a_supabase(tabla, df):
    df["datetime"] = df["datetime"].astype(str)
    df["extraction_timestamp"] = df["extraction_timestamp"].astype(str)
    df = df.where(pd.notnull(df), None)
    registros = df.to_dict(orient="records")
    supabase.table(tabla).insert(registros).execute()
  
#------------- Comprobación que está enviando correctamente los datos con documento log.write--------------

if __name__ == "__main__":
    datos = extraer_datos_ultimas_12h()
    for tabla, df in datos.items():
        subir_a_supabase(tabla, df)
    print("✅ Datos cargados correctamente.")


if __name__ == "__main__":
    try:
        datos = extraer_datos_ultimas_12h()
        for tabla, df in datos.items():
            subir_a_supabase(tabla, df)
        with open("C:\\Users\\luciv\\Desktop\\TRABAJO BOOTCAMP\\log_supabase.txt", "a") as log:
            log.write(f"[{datetime.now()}] ✅ Datos cargados correctamente.\n")
    except Exception as e:
        with open("C:\\Users\\luciv\\Desktop\\TRABAJO BOOTCAMP\\log_supabase.txt", "a") as log:
            log.write(f"[{datetime.now()}] ❌ ERROR: {str(e)}\n")


