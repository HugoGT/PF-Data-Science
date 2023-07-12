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


def insercion_postgres(querys):
    try:
        # Conexión a PostgreSQL
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password
        )
        cursor = conn.cursor()

        # Ejecutar las consultas de inserción
        for query in querys:
            cursor.execute(query)

        # Confirmar los cambios en la base de datos
        conn.commit()

        # Cerrar la conexión y el cursor
        cursor.close()
        conn.close()

        print("Se ejecutaron las sentencias correctamente")

    except (Exception, psycopg2.Error) as error:
        print("Error al ejecutar la inserción en PostgreSQL:", error)


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
    data = read_data_from_datalake("tabla_daños.csv")
    data.drop_duplicates(subset='id_danio', inplace=True)
    data['desaparecidos'] = data['desaparecidos'].astype(int)

    create_table = """DROP TABLE IF EXISTS danios;
                        CREATE TABLE danios(
                        id_danio SERIAL PRIMARY KEY,
                        fecha DATE,
                        afectados INT,
                        fallecidos INT,
                        desaparecidos INT,
                        heridos INT,
                        viviendas_destruidas INT,
                        viviendas_afectadas INT,
                        id_sismo INT,
                        id_lugar INT
                        );"""

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO danios (id_danio, fecha, afectados, fallecidos, desaparecidos, heridos, viviendas_destruidas, viviendas_afectadas, id_sismo, id_lugar) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        id_danio = row[0]
        fecha = row[1]
        afectados = row[2]
        fallecidos = row[3]
        desaparecidos = row[4]
        heridos = row[5]
        viv_destr = row[6]
        viv_afect = row[7]
        id_sismo = row[8]
        id_lugar = row[9]
        values = f"({id_danio}, '{fecha}', {afectados}, {fallecidos}, {desaparecidos}, {heridos}, {viv_destr}, {viv_afect}, {id_sismo}, {id_lugar}),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    insercion_postgres([create_table, query])


def load_lugar():
    # Insertar o actualizar los datos en PostgreSQL
    data = read_data_from_datalake("tabla_lugar.csv")

    create_table = """DROP TABLE IF EXISTS lugares;
                        CREATE TABLE lugares(
                        id_lugar SERIAL PRIMARY KEY,
                        pais VARCHAR(50),
                        estado VARCHAR(50)
                        );"""

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO lugares (id_lugar, pais, estado) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        id_lugar = row[0]
        pais = row[1]
        estado = row[2]
        values = f"({id_lugar}, '{pais}', '{estado}'),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    insercion_postgres([create_table, query])


def load_tsunamis():
    # Insertar o actualizar los datos en PostgreSQL
    data = read_data_from_datalake("tabla_tsunamis.csv")

    create_table = """DROP TABLE IF EXISTS tsunamis;
                        CREATE TABLE tsunamis(
                        id_tsunami SERIAL PRIMARY KEY,
                        fecha DATE,
                        hora TIME,
                        altura_de_ola DECIMAL,
                        id_sismo INT,
                        id_lugar INT
                        );"""

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO tsunamis (id_tsunami, fecha, hora, altura_de_ola, id_sismo, id_lugar) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        id_tsunami = row[0]
        fecha = row[1]
        hora = row[2]
        max_water_height = row[3]
        id_sismo = row[4]
        id_lugar = row[5]
        values = f"({id_tsunami}, '{fecha}', '{hora}', {max_water_height}, {id_sismo}, {id_lugar}),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    insercion_postgres([create_table, query])


def load_sismos():
    # Insertar o actualizar los datos en PostgreSQL
    data = read_data_from_datalake("tabla_sismos.csv")

    create_table = """DROP TABLE IF EXISTS sismos;
                        CREATE TABLE sismos(
                        id_sismo SERIAL PRIMARY KEY,
                        fecha DATE,
                        hora TIME,
                        latitud DECIMAL,
                        longitud DECIMAL,
                        profundidad DECIMAL,
                        magnitud DECIMAL,
                        id_lugar INT
                        );"""

    # Preparar la consulta SQL de inserción
    insert_query = "INSERT INTO sismos (fecha, hora, latitud, longitud, profundidad, magnitud, id_lugar) VALUES\n"
    values_query = []

    for index, row in data.iterrows():
        fecha = row[1]
        hora = row[2]
        latitud = row[3]
        longitud = row[4]
        profundidad = row[5]
        magnitud = row[6]
        id_lugar = row[7]
        values = f"('{fecha}', '{hora}', {latitud}, {longitud}, {profundidad}, {magnitud}, {id_lugar}),\n"
        values_query.append(values)

    query = insert_query + ''.join(values_query)[:-2] + ";"

    insercion_postgres([create_table, query])


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
