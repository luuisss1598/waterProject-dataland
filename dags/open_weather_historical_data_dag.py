from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.load_variables import get_env
from utils.logging_config import logger
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import os
import uuid
import time

# basics configurations for the batch processing
source_csv_path = './data/raw/open_weather_historical_data.csv'
temp_storage_path = './data/tmp/airflow'
batch_size = 25000
schema_name = 'open_weather_api'
table_name = 'temp_open_weather_api_historical_hourly'

raw_data_file_path = './data/raw/open_weather_historical_data.csv'

"""
1. define temp files in order to store data and load into
2. will break the csv files into chunks, perhaps 50k at a time
3. the way xcom works is by pushing and pulling data, small amount of data like metadata. task_ids refers to the task id create in the python operator,
    on the othe hand, you can also return the data from the function and it'll show up in the xcom tab.
"""

def split_csv_into_batches(**context):
    logger.info('Create unique tmp folder for data...')
    run_id = str(uuid.uuid4())# create a unique id for the file name
    run_id_dir = os.path.join(temp_storage_path, f'run_{run_id}')
    os.makedirs(run_id_dir, exist_ok=True)
    logger.info('Tmp folders created for data...')

    chunk_iterator = pd.read_csv(source_csv_path, chunksize=batch_size)
    batch_files = []

    logger.info('Split CSV into chunks for upload...')
    for i, chunk in enumerate(chunk_iterator):
        batch_file = os.path.join(run_id_dir, f'batch_{i:04d}.csv')
        chunk.to_csv(batch_file, index=False)
        batch_files.append(batch_file)
        logger.info(f'Saved batch {i} with {len(chunk)} rows to {batch_file}')
    
    metadata = {
        'run_id': run_id,
        'batch_files': batch_files,
        'total_batches': len(batch_files)
    }

    return metadata

def load_batches_to_postgres(**context):
    try:
        # pulled metadata from xcom and make sure is not empty
        meta_data =  context['ti'].xcom_pull(task_ids='split_csv_file')
        
        if meta_data is None:
            logger.error(f'Metadata is empty: {meta_data}')
            raise
    except Exception as err:
        logger.error(f'Metadata is empty: {err}')
        raise
    
    # get run_dir path
    run_dir = os.path.join(temp_storage_path, f'run_{meta_data["run_id"]}')
    
    # logger.info(f'Pulled metadata: {json.dumps(meta_data, indent=4)}')
    logger.info(f'Pulled metadata')
    
    password = get_env('NEON_POSTGRES_PASSWORD')
    user = get_env('NEON_POSTGRES_USER')
    host = get_env('NEON_POSTGRES_HOST')
    port = get_env('NEON_POSTGRES_PORT')
    database = get_env('NEON_POSTGRES_DB_INGEST')
    
    # connection string needed to pass into engine to connect to postgres
    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    
    engine = create_engine(conn_str)
    
    with engine.connect() as conn:
        try:
            conn.execute('select 1')
            logger.info('Connection was successful...')
        except Exception as err:
            logger.error('Connection Failed: {err}')
            raise
    
    start_time = time.time()
    logger.info(f'Starting batch loading into Postgres...')
    total_files_read = 0
    for root, dirs, files in os.walk(run_dir):
        # make sure to sort files from 00 to len(files)-1
        for file in sorted(files):
            full_path = os.path.join(root, file)
            df = pd.read_csv(full_path)
            
            try:
                df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False)
                logger.info(f'{len(df)} rows ingested from batch {file} into table {table_name}')
            except Exception as err:
                logger.info(f'Failed to load data to Postgres: {err}')
                raise
            
            total_files_read += 1
                        
    end_time = time.time()
    elapsed_time = (end_time-start_time)
    logger.info(f'Total files loaded into Postgres db: {total_files_read}')
    logger.info(f'Time timen to ingest data: {elapsed_time}s')
    
    # i need to return metadata in order to find run_id directory and clean/remove/delete temp files
    return meta_data


def clean_up(**context):
    load_result = context['ti'].xcom_pull(task_ids='load_to_postgres')

    logger.info('Starting cleanup process...')
    if load_result and 'run_id' in load_result:
        run_dir = os.path.join(temp_storage_path, f'run_{load_result['run_id']}')

        for root, dirs, files in os.walk(run_dir):
            for file in files:
                os.remove(os.path.join(root, file))

        os.rmdir(run_dir)
        
    logger.info('Cleanup process finalized...')
    
    return load_result['run_id']

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": "lherrera5034@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    'ingest_open_weather_historical_data',
    default_args=default_args,
    description='EL pipeline to load historical CSV data from Open Weather into Postgres DB.',
    schedule=None,
    start_date=datetime(2025, 3, 17),
    catchup=False,
    tags=['historical_data', 'csv_bulk']
) as dag:
    task_1 = PythonOperator(
        task_id='split_csv_file',
        python_callable=split_csv_into_batches
    ) 

    task_2 = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_batches_to_postgres
    )

    task_3 = PythonOperator(
        task_id='cleanup_directories',
        python_callable=clean_up
    )

    task_1 >> task_2 >> task_3;