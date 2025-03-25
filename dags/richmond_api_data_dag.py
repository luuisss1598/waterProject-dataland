from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import uuid
import os

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": "lherrera5034@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)  # Fixed typo here (removed the line break)
}

API_ENDPOINTS = {
    'richmond_water_consumption_data': 'https://www.transparentrichmond.org/resource/xivi-n6g4.json',
    'water_consumption': 'https://www.transparentrichmond.org/resource/98sv-3bgz.json',
    'single_family_water_consumption': 'https://www.transparentrichmond.org/resource/5wpn-bryw.json',
    'multi_family_water_consumption': 'https://www.transparentrichmond.org/resource/8dqm-egfs.json',
    'municipal_family_utility_usage': 'https://www.transparentrichmond.org/resource/9v6a-54wj.json'
}

RAW_DATA_SOURCE_PATH: str = './data/raw/water_quality/water_quality_lab_results.csv'
TEMP_STORAGE_PATH: str = './data/processed/richmond_catalog_api'
SCHEMA_NAME: str = 'water_quality'

# make sure temp file exist
os.makedirs(TEMP_STORAGE_PATH, exist_ok=True)

def extract_richmond_catalog_api(api_endpoint: str, **context):
    run_id = str(uuid.uuid4()) # create unique_id for temp json file
    run_id_file_path = os.path.join(TEMP_STORAGE_PATH, f'run_{run_id}.json')
    
    response = requests.get(url=api_endpoint, timeout=5)
    data = response.json()
    
    # create dataframe
    df = pd.DataFrame(data)
    
    # create the file, the return all metadata for file
    df.to_json(run_id_file_path,index=False)
    
    metadata_api = {
        'run_id': run_id,
        'file_path': run_id_file_path
    }
    
    return metadata_api

def transform_richmond_catalog_api(**context):
    ti = context['ti']
    
    t1 = ti.xcom_pull(task_ids='extract_water_consumption')
    t2 = ti.xcom_pull(task_ids='extract_single_family_water_consumption')
    t3 = ti.xcom_pull(task_ids='extract_richmond_water_consumption_data')
    t4 = ti.xcom_pull(task_ids='extract_municipal_family_utility_usage')
    t5 = ti.xcom_pull(task_ids='extract_multi_family_water_consumption')
    
    
    extract_metadata = [t1, t2, t3, t4, t5]
    
    return {
        'extract_water_consumption': t1,
        'extract_single_family_water_consumption': t2,
        'extract_richmond_water_consumption_data': t3,
        'extract_municipal_family_utility_usage': t4,
        'extract_multi_family_water_consumption': t5,
    }
    
def load_richmond_catalog_api(task_id: str, **context):
    ti = context['ti']
    transformed_metadata = ti.xcom_pull(task_ids='transform_all_data_sources')
    
    return {
        'task_id': task_id
    }

with DAG(
    'ingest_richmond_api_data',
    default_args=default_args,
    description='Data pipeline to ingest data from Richmond data catalog, multiple endpoints.',
    schedule_interval=None,
    start_date=datetime(2025, 3, 24),
    catchup=False,
    tags=['water', 'api']
) as dag:
    
    water_consumption_task = PythonOperator(
        task_id='extract_water_consumption',
        python_callable=extract_richmond_catalog_api,
        op_kwargs={
            'api_endpoint': API_ENDPOINTS['water_consumption']
        }
    )

    single_family_water_consumption_task = PythonOperator(
        task_id='extract_single_family_water_consumption',
        python_callable=extract_richmond_catalog_api,
        op_kwargs={
            'api_endpoint': API_ENDPOINTS['single_family_water_consumption']
        }
    )

    richmond_water_consumption_data_task = PythonOperator(
        task_id='extract_richmond_water_consumption_data',
        python_callable=extract_richmond_catalog_api,
        op_kwargs={
            'api_endpoint': API_ENDPOINTS['richmond_water_consumption_data']
        }
    )

    municipal_family_utility_usage_task = PythonOperator(
        task_id='extract_municipal_family_utility_usage',
        python_callable=extract_richmond_catalog_api,
        op_kwargs={
            'api_endpoint': API_ENDPOINTS['municipal_family_utility_usage']
        }
    )

    multi_family_water_consumption_task = PythonOperator(
        task_id='extract_multi_family_water_consumption',
        python_callable=extract_richmond_catalog_api,
        op_kwargs={
            'api_endpoint': API_ENDPOINTS['multi_family_water_consumption']
        }
    )
    
    transform_all_data_sources_task = PythonOperator(
        task_id='transform_all_data_sources',
        python_callable=transform_richmond_catalog_api
    )

    load_water_consumption_task = PythonOperator(
        task_id='load_water_consumption',
        python_callable=load_richmond_catalog_api,
        op_kwargs={
            'task_id': 'extract_water_consumption'
        }
    )

    load_single_family_water_consumption_task = PythonOperator(
        task_id='load_single_family_water_consumption',
        python_callable=load_richmond_catalog_api,
        op_kwargs={
            'task_id': 'extract_single_family_water_consumption'
        }
    )


    [
        water_consumption_task, single_family_water_consumption_task, 
        richmond_water_consumption_data_task, municipal_family_utility_usage_task, 
        multi_family_water_consumption_task
    ] >> transform_all_data_sources_task >> [
        load_water_consumption_task, 
        load_single_family_water_consumption_task
    ]