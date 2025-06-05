import os
from dotenv import load_dotenv
import psycopg2

# Cargar el archivo .env
load_dotenv()

host = os.getenv("SUPABASE_DB_HOST")
port = os.getenv("SUPABASE_DB_PORT")
dbname = os.getenv("SUPABASE_DB_NAME")
user = os.getenv("SUPABASE_DB_USER")
password = os.getenv("SUPABASE_DB_PASSWORD")


try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    print("Conectado a:", cursor.fetchone())

    cursor.close()
    conn.close()
except Exception as e:
    print("Error al conectar:", e)
