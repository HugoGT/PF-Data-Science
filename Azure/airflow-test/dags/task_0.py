# Tarea que corre una vez antes de levantar todo airflow

import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta

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


def read_data_from_datalake(blob_name):
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall().decode("utf-8")
    df = pd.read_csv(pd.io.common.StringIO(data))

    return df


def load_daños():
    # Insertar o actualizar los datos en PostgreSQL
    data = read_data_from_datalake("daños_final.csv")
    data.drop_duplicates(subset='id_danio', inplace=True)

    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO danios (id_danio, fecha, estado, afectados, desaparecidos, heridos, viv_destr, viv_afect, id_sismo) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        id_danio = row[0]
        fecha = row[1]
        estado = row[2]
        afectados = row[3]
        desaparecidos = row[4]
        heridos = row[5]
        viv_destr = row[6]
        viv_afect = row[7]
        id_sismo = row[8]
        values = f"({id_danio}, '{fecha}', '{estado}', {afectados}, {desaparecidos}, {heridos}, {viv_destr}, {viv_afect}, {id_sismo}),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    # Ejecutar la consulta de inserción
    cursor.execute(query)

    # Cerrar la conexión a PostgreSQL
    conn.commit()
    cursor.close()
    conn.close()


def load_lugar():
    # Insertar o actualizar los datos en PostgreSQL
    data = read_data_from_datalake("lugar.csv")

    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO lugar (id_lugar, pais, estado) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        id_lugar = row[0]
        pais = row[1]
        estado = row[2]
        values = f"({id_lugar}, '{pais}', '{estado}'),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    # Ejecutar la consulta de inserción
    cursor.execute(query)

    # Cerrar la conexión a PostgreSQL
    conn.commit()
    cursor.close()
    conn.close()


def load_tsunamis():
    # Insertar o actualizar los datos en PostgreSQL
    data = read_data_from_datalake("tsunamis.csv")

    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO tsunamis (id_tsunami, fecha, hora, max_water_height, id_sismo, estado, pais) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        id_tsunami = row[0]
        fecha = row[1]
        hora = row[2]
        max_water_height = row[3]
        id_sismo = row[4]
        estado = row[5]
        pais = row[6]
        values = f"({id_tsunami}, '{fecha}', '{hora}', {max_water_height}, {id_sismo}, '{estado}', '{pais}'),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    # Ejecutar la consulta de inserción
    cursor.execute(query)

    # Cerrar la conexión a PostgreSQL
    conn.commit()
    cursor.close()
    conn.close()


def load_sismos():
    # Insertar o actualizar los datos en PostgreSQL
    data = read_data_from_datalake("sismos_completo.csv")

    # Conexión a PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO sismos (fecha, hora, latitud, longitud, profundidad, magnitud, pais, estado) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        fecha = row[1]
        hora = row[2]
        latitud = row[3]
        longitud = row[4]
        profundidad = row[5]
        magnitud = row[6]
        pais = row[7]
        estado = row[8]
        values = f"('{fecha}', '{hora}', {latitud}, {longitud}, {profundidad}, {magnitud}, '{pais}', '{estado}'),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    # Ejecutar la consulta de inserción
    cursor.execute(query)

    # Cerrar la conexión a PostgreSQL
    conn.commit()
    cursor.close()
    conn.close()


# Definir el DAG de Airflow
default_args = {
    'owner': 'Hugo',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'catchup': False,
    'max_active_runs': 1
}


with DAG(
    'update_all_tables',
    description='Sube todas las tablas a PostgreSQL',
    start_date=datetime.now() - timedelta(days=1),
    schedule="@once",
    default_args=default_args,
) as dag:
    t1 = PythonOperator(
        task_id='load_daños',
        python_callable=load_daños
    )
    t2 = PythonOperator(
        task_id='load_lugar',
        python_callable=load_lugar
    )
    t3 = PythonOperator(
        task_id='load_tsunamis',
        python_callable=load_tsunamis
    )
    t4 = PythonOperator(
        task_id='load_sismos',
        python_callable=load_sismos
    )

    t1 >> t2 >> t3 >> t4
