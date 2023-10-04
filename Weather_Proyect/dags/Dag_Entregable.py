from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from  Clima_API import Extract_weather_data,Transform_data,redshift_connection,insert_data
from airflow.models import TaskInstance

import http.client
import os
import json

# Obtén la ruta completa del directorio actual donde se encuentra el DAG
current_dir = os.path.dirname(os.path.abspath(__file__))

# Combina la ruta del directorio actual con el nombre del archivo JSON
json_file_path = os.path.join(current_dir, "config.json")

# Abre el archivo JSON utilizando la ruta completa
with open(json_file_path) as config_file:
    config_data = json.load(config_file)

api_key = config_data["API_KEY"]  # Reemplaza "api_key" con el nombre de tu clave en el JSON
redshift_key = config_data["Reshift_KEY"]  # Reemplaza "redshift_key" con el nombre de tu clave en el JSON
redshift_user = config_data["Redshift_User"]  # Reemplaza "redshift_user" con el nombre de tu clave en el JSON




default_args = {
    'owner': 'Joel_Luis',
    'email': 'joel.luis.c@gmail.com',
    'start_date': datetime(2023, 9, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ingestion_dag = DAG(
    dag_id='ingestion_data',
    default_args=default_args,
    description='Agrega datos del clima de ciudades turisticas de Argentina',
     schedule_interval=timedelta(days=1),
    catchup=False
)


task_1 = PythonOperator(
    task_id='extract_data_from_api',
    python_callable=Extract_weather_data,
    op_args=[api_key],  # Pasa api_key como argumento
    dag=ingestion_dag
)

task_2 = PythonOperator(
    task_id='transform_data',
    op_args=[api_key],
    python_callable=Transform_data,
    dag=ingestion_dag
)

task_3 = PythonOperator(
    task_id='create_connection',
    python_callable=redshift_connection,
    op_args=[redshift_key, redshift_user],  # Pasa las claves de Redshift como argumentos
    dag=ingestion_dag
)

task_4 = PythonOperator(
    task_id='load_data',
    python_callable=insert_data,
    op_args=[redshift_key, redshift_user,api_key],
    dag=ingestion_dag
)

task_1 >> task_2 >> task_3 >> task_4