# Primera tarea de Airflow

from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
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
    latitud = row['latitud']
    longitud = row['longitud']

    return f'{latitud},{longitud}'


def obtener_ultima_fila():
    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Ejecutar la consulta
    consulta = "SELECT * FROM sismos ORDER BY id_sismo DESC LIMIT 1"
    cursor.execute(consulta)

    # Obtener la última fila de resultados
    ultima_fila = cursor.fetchone()

    # Cerrar la conexión a PostgreSQL
    cursor.close()
    conn.close()

    ultima_fila = [str(i) for i in ultima_fila][:-3]
    ultima_fila = ultima_fila[:2] + [float(i) for i in ultima_fila[2:]]

    return ultima_fila


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

    # Crear el Diccionario usando las listas de datos
    data = {
        'fecha': fechas,
        'latitud': latitudes,
        'longitud': longitudes,
        'profundidad': profundidades,
        'magnitud': magnitudes
    }

    Variable.set('data', data)


def transformar_data():
    # Obtener data extraída
    ultima_fila = eval(Variable.get('last_row'))

    # Si no existe la variable en Airflow se crea una
    if ultima_fila == []:
        ultima_fila = obtener_ultima_fila()
        Variable.set('last_row', ultima_fila)

    data = eval(Variable.get('data'))
    df = pd.DataFrame(data)

    # Cambio el formato de Fecha
    df[['fecha', 'hora']] = df['fecha'].str.split(' ', 1, expand=True)
    # Convertir la columna 'Fecha' a tipo datetime
    df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%b-%Y')
    # Reordenar las columnas
    df = df[['fecha', 'hora', 'latitud', 'longitud', 'profundidad', 'magnitud']]
    # Convertir tipos de datos
    df['fecha'] = df['fecha'].astype(str)
    df['profundidad'] = df['profundidad'].astype(int)
    df[['latitud', 'longitud', 'magnitud']] = df[['latitud', 'longitud', 'magnitud']].astype(float)

    # Comprobar datos faltantes
    for index, row in df.iterrows():
        if row.tolist() == ultima_fila:
            df = df.iloc[:index]
            break

    if df.empty:
        return Variable.set('query', ' ')

    Variable.set('last_row', df.iloc[0].tolist())

    # Conversiones ETL perú
    geolocator = Nominatim(user_agent="test", timeout=4)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

    # Aplica la función a cada fila del DataFrame y crea la nueva columna
    coordenadas = df.apply(combinar_coordenadas, axis=1).tolist()

    ubicacion = []
    for u in coordenadas:
        ubicacion.append(geolocator.reverse(u))

    paises = []
    estados = []

    for item in ubicacion:
        if item == None:
            paises.append('Perú')
            estados.append('Mar peruano')
        else:
            address = item.raw['address']

            paises.append(address.get('country'))
            estados.append(address.get('state'))

    # Agrego los datos al df
    df['pais'] = paises
    df['estado'] = estados

    # Relleno nulos
    df['estado'] = df['estado'].fillna('Mar peruano')

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO sismos (fecha, hora, latitud, longitud, profundidad, magnitud, pais, estado) VALUES\n"
    values_query = []

    for index, row in df[::-1].iterrows():
        fecha = row[0]
        hora = row[1]
        latitud = row[2]
        longitud = row[3]
        profundidad = row[4]
        magnitud = row[5]
        pais = row[6]
        estado = row[7]

        values = f"('{fecha}', '{hora}', {latitud}, {longitud}, {profundidad}, {magnitud}, '{pais}', '{estado}'),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    Variable.set('query', query)


def subir_data():
    # Recibir la query a subir
    query = Variable.get('query', ' ')

    # Si no hay query es porque no hay datos nuevos
    if query == ' ':
        return print('No hay nada que añadir')

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
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

with DAG(
    'datalake_to_postgresql',
    default_args=default_args,
    start_date=datetime.now() - timedelta(hours=6),
    schedule='0 * * * *'
) as dag:
    extract_task = PythonOperator(
        task_id='extraer_data',
        python_callable=extraer_data
    )
    transform_task = PythonOperator(
        task_id='transformar_data',
        python_callable=transformar_data
    )
    load_task = PythonOperator(
        task_id='subir_data',
        python_callable=subir_data
    )

    extract_task >> transform_task >> load_task
