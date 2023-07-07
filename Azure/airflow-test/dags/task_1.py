# Primera tarea de Airflow

import random
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim

from keys import (
    PGHOST,
    PGUSER,
    PGPORT,
    PGDATABASE,
    PGPASSWORD,
    AZURE_STORAGE_NAME,
    AZURE_STORAGE_KEY,
    AZURE_CONTAINER_NAME,
    AZURE_BLOB_NAME
)


# Datos de conexión a PostgreSQL
pg_host = PGHOST
pg_user = PGUSER
pg_port = PGPORT
pg_database = PGDATABASE
pg_password = PGPASSWORD

# Datos de conexión al datalake
storage_account_name = AZURE_STORAGE_NAME
storage_account_key = AZURE_STORAGE_KEY
container_name = AZURE_CONTAINER_NAME
blob_name = AZURE_BLOB_NAME


def combinar_coordenadas(row):
    latitud = row['latitud (º)']
    longitud = row['longitud (º)']
    coordenadas = f'{latitud},{longitud}'

    return coordenadas


def extraer_data():
    # Pedido de datos a la pagina
    response = requests.get("https://ds.iris.edu/latin_am/evlist.phtml?region=peru")
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    tabla = soup.find('table', id='evTable')

    # Encuentra todas las filas de la tabla en el cuerpo del documento
    filas = tabla.tbody.find_all('tr')

    # Crear listas vacías para almacenar los datos
    fechas = []
    latitudes = []
    longitudes = []
    magnitudes = []
    profundidades = []

    # Recorre las filas e guarda los datos en las listas correspondientes
    for fila in filas:
        celdas = fila.find_all('td')
        fechas.append(celdas[0].text.strip())
        latitudes.append(celdas[1].text.strip())
        longitudes.append(celdas[2].text.strip())
        profundidades.append(celdas[4].text.strip())
        magnitudes.append(celdas[3].text.strip())

    # Crear el DataFrame usando las listas de datos
    data = {
        'fecha UTC': fechas,
        'latitud (º)': latitudes,
        'longitud (º)': longitudes,
        'profundidad (km)': profundidades,
        'magnitud (M)': magnitudes
    }

    Variable.set('data', data)


def subir_data():
    # Obtener data extraída
    data = Variable.get('data')
    df = pd.DataFrame(data)

    # Cambio el formato de Fecha
    df[['fecha UTC', 'hora UTC']] = df['fecha UTC'].str.split(' ', 1, expand=True)
    # Convertir la columna 'Fecha' a tipo datetime
    df['fecha UTC'] = pd.to_datetime(df['fecha UTC'], format='%d-%b-%Y')
    # Cambiar el formato de la fecha a "02/07/2023"
    df['fecha UTC'] = df['fecha UTC'].dt.strftime('%d/%m/%Y')
    # Reordenar las columnas
    df = df[['fecha UTC', 'hora UTC', 'latitud (º)', 'longitud (º)', 'profundidad (km)', 'magnitud (M)']]

    # Conversiones ETL perú
    geolocator = Nominatim(user_agent="test", timeout=4)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

    # Aplica la función a cada fila del DataFrame y crea la nueva columna
    coordenadas = df.apply(combinar_coordenadas, axis=1).to_list()

    ubicacion = []
    for u in coordenadas:
        ubicacion.append(geolocator.reverse(u))

    paises = []
    departamentos = []
    provincias = []

    for item in ubicacion:
        if item == None:
            paises.append('Perú')
            departamentos.append('Mar peru')
            provincias.append(None)
        else:
            address = item.raw['address']

            paises.append(address.get('country'))
            departamentos.append(address.get('state'))
            provincias.append(address.get('region'))

    # Agrego los datos al df
    df['pais'] = paises
    df['departamento'] = departamentos
    df['provincia'] = provincias

    # Renombro columnas
    df = df.rename(columns={
        'fecha UTC': 'fecha',
        'hora UTC': 'hora',
        'latitud (º)': 'latitud',
        'longitud (º)': 'longitud',
        'profundidad (km)': 'profundidad',
        'magnitud (M)': 'magnitud',
        'departamento': 'estado',
        'provincia': 'ciudad'
    })

    # Cambio formatos
    df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y')
    df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
    df['hora'] = df['hora'].apply(lambda x: x.split('.')[0])

    # Relleno nulos
    df['estado'] = df['estado'].fillna('Mar peruano')
    df['ciudad'] = df['ciudad'].fillna('Sin dato')

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO sismos (id_sismo, fecha, hora, latitud, longitud, profundidad, magnitud, pais, estado) VALUES\n"
    values_query = []

    for index, row in df.iterrows():
        fecha = row[0]
        hora = row[1]
        latitud = row[2]
        longitud = row[3]
        profundidad = row[4]
        magnitud = row[5]
        pais = row[6]
        estado = row[7]

        values = f"({index + 44978}, '{fecha}', '{hora}', {latitud}, {longitud}, {profundidad}, {magnitud}, '{pais}', '{estado}'),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Ejecutar la consulta de inserción
    cursor.execute(query)
    conn.commit()

    # Cerrar la conexión a PostgreSQL
    cursor.close()
    conn.close()


# Definir el DAG de Airflow
default_args = {
    'owner': 'Hugo',
    'depends_on_past': False,
    'email': ['gthugo@outlook.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
    'datalake_to_postgresql',
    default_args=default_args,
    start_date=datetime.now() - timedelta(minutes=5),
    schedule='*/5 * * * *'
) as dag:
    extract_task = PythonOperator(
        task_id='extraer_data',
        python_callable=extraer_data
    )
    load_task = PythonOperator(
        task_id='subir_data',
        python_callable=subir_data
    )

    extract_task >> load_task
