# Tarea que corre una vez antes de levantar todo airflow

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


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
    'every_minute_dag',
    description='Mi prueba de dag',
    start_date=datetime.now() - timedelta(minutes=8),
    schedule="* * * * *",
    default_args=default_args,
) as dag:
    t1 = BashOperator(
        task_id='hello',
        bash_command="echo 'Works'"
    )

    t1
