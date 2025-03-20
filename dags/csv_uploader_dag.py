from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extractors.extract_and_split_csv_files import split_csv_into_batches
from loaders.load_csv_file_to_db import load_batches_into_postgres
from utils.clean_dir_files import clean_up
# import json

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": "lherrera5034@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

RAW_DATA_SOURCE_PATH: str = './data/raw/open_weather_historical_data.csv'
TEMP_STORAGE_PATH: str = './data/tmp/airflow'
BATCH_SIZE: int = 25000
SCHEMA_NAME: str = 'open_weather_api'
TABLE_NAME: str = 'temp2_open_weather_api_historical_hourly'


"""-------------------------- Dag functions Start --------------------------"""

def csv_to_batches(**context):
    # ti = context['ti']
    metadata: dict =  split_csv_into_batches(
        raw_data_source_file_path=RAW_DATA_SOURCE_PATH,
        temp_storage_path=TEMP_STORAGE_PATH, 
        batch_size=BATCH_SIZE
    )
    
    return metadata


def load_batches_to_db(**context):
    ti = context['ti']

    metadata = ti.xcom_pull(task_ids='split_data')

    metadata_load = load_batches_into_postgres(
        meta_data=metadata,
        temp_storage_path=TEMP_STORAGE_PATH,
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME
    )

    return metadata_load

def clean_up_dic(**context):
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='load_data')

    metadata_clean = clean_up(
        meta_data=metadata,
        temp_storage_path=TEMP_STORAGE_PATH
    )

    return metadata_clean

"""-------------------------- Dag functions End --------------------------"""

"""-------------------------- Airflow Dag --------------------------"""
# follow the N-Da-D-S-St-C-T pattern
with DAG(
    'ingest_csv_data_to_postgres',
    default_args=default_args,
    description='Pipeline to ingest data from CSV file into Postgres DB.',
    schedule=None,
    start_date=datetime(2025, 3, 20),
    catchup=False,
    tags=['csv_files']
) as dag:
    """-------------------------- Tasks --------------------------"""
    split_data_task = PythonOperator(
        task_id='split_data',
        python_callable=csv_to_batches,
    )

    load_data_db = PythonOperator(
        task_id='load_data',
        python_callable=load_batches_to_db,
    )

    clean_temp_dir = PythonOperator(
        task_id='clean_up_data',
        python_callable=clean_up_dic
    )

    split_data_task >> load_data_db >> clean_temp_dir;