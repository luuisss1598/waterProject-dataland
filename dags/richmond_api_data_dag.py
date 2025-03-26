from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.logging_config import logger
from utils.load_variables import get_env
import requests
from sqlalchemy import create_engine
import pandas as pd
import uuid
import os
import re

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": "lherrera5034@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)  # Fixed typo here (removed the line break)
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
SCHEMA_NAME: str = 'richmond_water_data'

# make sure temp file exist
os.makedirs(TEMP_STORAGE_PATH, exist_ok=True)

def extract_richmond_catalog_api(api_endpoint: str, **context):
    run_id = str(uuid.uuid4()) # create unique_id for temp json file
    run_id_file_path = os.path.join(TEMP_STORAGE_PATH, f'run_{run_id}.json')
    
    try:
        logger.info('Sending request to API...')
        response = requests.get(url=api_endpoint, timeout=10)
        response.raise_for_status() # raise exception if not 200 status code
        
        logger.info('API call was successful...')
        data = response.json()
        
        # create dataframe
        logger.info('Creating dataframe for data...')
        df = pd.DataFrame(data)
    except requests.exceptions.HTTPError as http_err:
        logger.error(f'HTTP error: {http_err}')
        raise
        
    logger.info('Dumping data into json file...')
    df.to_json(run_id_file_path,index=False)
    
    metadata_api = {
        'raw_run_id': run_id,
        'raw_file_path': run_id_file_path
    }
    
    return metadata_api

def transform_richmond_catalog_api(**context):

    """----------------------- load metadata from xcom -----------------------"""
    ti = context['ti']
    
    logger.info('Extracting XCOM metadata...')    
    t1 = ti.xcom_pull(task_ids='extract_water_consumption')
    t2 = ti.xcom_pull(task_ids='extract_single_family_water_consumption')
    t3 = ti.xcom_pull(task_ids='extract_richmond_water_consumption_data')
    t4 = ti.xcom_pull(task_ids='extract_municipal_family_utility_usage')
    t5 = ti.xcom_pull(task_ids='extract_multi_family_water_consumption')

    # centralized returned metadata from all endpoints
    extract_tasks_metadata = [t1, t2, t3, t4, t5]

    """----------------------- transform json into csv -----------------------"""
    
    # transformed_metadata = {} # for future reference, we can create metadata in the for-loop, just need to pass task_id
    logger.info('Starting transformation process...')
    for task in extract_tasks_metadata:
        run_id = str(uuid.uuid4())
        raw_run_id_file_path:str = task['raw_file_path']
        transformed_run_id_file_path:str = os.path.join(TEMP_STORAGE_PATH, f'run_{run_id}.csv')
        
        # transform into dataframe 
        logger.info('Transforming json data into dataframe...')
        df = pd.read_json(raw_run_id_file_path)

        # dump into csv file, drop index
        logger.info('Transforming datframe into csv file...')
        df.to_csv(transformed_run_id_file_path, index=False)

        # append transformed file to t1, t2, t+1
        logger.info('Creating metadata...')
        task['transformed_file_path'] = transformed_run_id_file_path
        task['transformed_run_id'] = run_id

    """----------------------- return new metadata for loading -----------------------"""
    logger.info('Transforming has been completed..')
    return {
        'extract_water_consumption': t1,
        'extract_single_family_water_consumption': t2,
        'extract_richmond_water_consumption_data': t3,
        'extract_municipal_family_utility_usage': t4,
        'extract_multi_family_water_consumption': t5,
    }
    
def load_richmond_catalog_api(task_id: str, table_name: str, schema_name: str, **context):
    """----------------------- load all metadata returned by transformation -----------------------"""
    logger.info('Extracting XCOM metadata...')    
    ti = context['ti']
    transformed_metadata = ti.xcom_pull(task_ids='transform_all_data_sources')

    # load specific task from extraction to be loaded into db
    load_extracted_task = transformed_metadata[f'{task_id}']

    """----------------------- load data to postgres db -----------------------"""
    # load env variables
    password = get_env('NEON_POSTGRES_PASSWORD')
    user = get_env('NEON_POSTGRES_USER')
    host = get_env('NEON_POSTGRES_HOST')
    port = get_env('NEON_POSTGRES_PORT')
    database = get_env('NEON_POSTGRES_DB_INGEST')

    logger.info('Creating database connection to Postgres...')    
    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

    engine = create_engine(conn_str)

    """ connection checking """
    with engine.connect() as conn:
        try:
            logger.info('Checking connection...')
            conn.execute('select 1')
            logger.info('Connection was successful...')
        except Exception as err:
            logger.error(f'Error connecting to database: {err}')
            raise
    
    # read-in transformed data
    transformed_data_file_path = load_extracted_task['transformed_file_path']
    df = pd.read_csv(transformed_data_file_path)

    """ ddl query execution """
    # check if table exist, if not then create table, no need for schema as we'll manually create it (might update later)
    with engine.connect() as conn:
        try:
            logger.info('Checking if table exist, if not it will be created...')
            ddl_query = pd.io.sql.get_schema(df, name=table_name, schema=schema_name, con=engine)
            ddl_query_if_does_not_exist = ddl_query.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS') # manually add the 'if does not exist' statement
            logger.info('Executing DDL process...')
            conn.execute(ddl_query_if_does_not_exist)
            logger.info('DDL executed successfully...')
        except Exception as err:
            logger.error(f'Error creating table: {err}')
            raise

    """ loading/ingestion execution """
    try:
        logger.info('Cleaning up dataframe columns before ingestion...')
        df.columns = [re.sub(r'\W+', '_', col) for col in df.columns] # clean columns
        df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='replace', index=False)
        logger.info(f'{len(df)} rows ingested from file {transformed_data_file_path} into table {table_name}')
    except Exception as err:
        logger.info(f'Failed to load data to Postgres: {err}')
        raise


    """----------------------- return new metadata for cleanup per task loaded -----------------------"""
    # return task id and all give information, from raw file path to transformed file path
    logger.info(f'Returning metadata from task_id: {task_id}')
    return {
        f'{task_id}': load_extracted_task
    }

def clean_up_temp_files(**context):
    ti = context['ti']
    
    t1 = ti.xcom_pull(task_ids='load_water_consumption')
    t2 = ti.xcom_pull(task_ids='load_single_family_water_consumption')
    t3 = ti.xcom_pull(task_ids='load_richmond_water_consumption_data')
    t4 = ti.xcom_pull(task_ids='load_municipal_family_utility_usage')
    t5 = ti.xcom_pull(task_ids='load_multi_family_water_consumption')

    # centralized returned metadata from all loading tasks
    loading_tasks_metadata = [t1, t2, t3, t4, t5]
    temp_files_removed = []
    for task_id in loading_tasks_metadata:
        # i need to iterate through the keys first
        for key in task_id.keys():
            raw_temp_file_path= task_id[key]['raw_file_path']
            transformed_temp_file_path= task_id[key]['transformed_file_path']

            os.remove(raw_temp_file_path)
            os.remove(transformed_temp_file_path)
            
            # create a dict of tuples, perfect match for both files and key being intial task id from extraction
            temp_key_file_dict = {
                key: (raw_temp_file_path, transformed_temp_file_path)
            }

            temp_files_removed.append(temp_key_file_dict)

    return {
        'files_removed': temp_files_removed
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
            'task_id': 'extract_water_consumption',
            'table_name': 'raw_water_consumption',
            'schema_name': f'{SCHEMA_NAME}'
        }
    )

    load_single_family_water_consumption_task = PythonOperator(
        task_id='load_single_family_water_consumption',
        python_callable=load_richmond_catalog_api,
        op_kwargs={
            'task_id': 'extract_single_family_water_consumption',
            'table_name': 'raw_single_family_water_consumption',
            'schema_name': f'{SCHEMA_NAME}'
        }
    )

    load_richmond_water_consumption_data_task = PythonOperator(
        task_id='load_richmond_water_consumption_data',
        python_callable=load_richmond_catalog_api,
        op_kwargs={
            'task_id': 'extract_richmond_water_consumption_data',
            'table_name': 'raw_richmond_water_consumption_data',
            'schema_name': f'{SCHEMA_NAME}'
        }
    )

    load_municipal_family_utility_usage_task = PythonOperator(
        task_id='load_municipal_family_utility_usage',
        python_callable=load_richmond_catalog_api,
        op_kwargs={
            'task_id': 'extract_municipal_family_utility_usage',
            'table_name': 'raw_municipal_family_utility_usage',
            'schema_name': f'{SCHEMA_NAME}'
        }
    )

    load_multi_family_water_consumption_task = PythonOperator(
        task_id='load_multi_family_water_consumption',
        python_callable=load_richmond_catalog_api,
        op_kwargs={
            'task_id': 'extract_multi_family_water_consumption',
            'table_name': 'raw_multi_family_water_consumption',
            'schema_name': f'{SCHEMA_NAME}'
        }
    )

    clean_up_all_temp_files_task = PythonOperator(
        task_id='clean_up_all_temp_files',
        python_callable=clean_up_temp_files
    )

    # group all extract tasks
    extract_tasks =   [
        water_consumption_task, single_family_water_consumption_task, 
        richmond_water_consumption_data_task, municipal_family_utility_usage_task, 
        multi_family_water_consumption_task
    ]

    # group all load tasks
    load_tasks = [
        load_water_consumption_task, 
        load_single_family_water_consumption_task, 
        load_richmond_water_consumption_data_task, 
        load_multi_family_water_consumption_task, 
        load_municipal_family_utility_usage_task
    ]

    # final definition for pipeline
    extract_tasks >> transform_all_data_sources_task >> load_tasks >> clean_up_all_temp_files_task;