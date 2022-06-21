from spotify_etl import spotify_etl
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Ich habe mich entschieden den gesamten ETL Prozess in einem Task auszuführen, da
# das Skript so sehr gut funktioniert. Alternativ kann man das Skript in 2 oder 3
# Tasks aufbrechen, wodurch der Graph in Airflow mehr Knoten hätte.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,6,21),
    'email': [-------],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='',
    schedule_interval=timedelta(days=1),
)

etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=spotify_etl,
    dag=dag,
)

etl
