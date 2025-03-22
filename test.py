from utils.load_variables import get_env
from utils.logging_config import logger
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import os
import uuid
import time
from typing import Any, Dict
import json 
import re


source_csv_path = './data/raw/open_weather_historical_data.csv'
temp_storage_path = './data/tmp/airflow'
batch_size = 25000
schema_name = 'open_weather_api'
table_name = 'temp_open_weather_api_historical_hourly'

# raw_data_source_file_path = './data/raw/open_weather_historical_data.csv'
raw_data_source_file_path = './data/raw/station_5.csv'

"""
1. create unique id for temp folder -> run_id
2. join raw_data_source_file_path with run_id
3. make sure to create or check if previous dir exist
4. create chunk iterator, initlize batch files
"""

def split_csv_into_batches(raw_data_source_file_path: str, temp_storage_path: str, batch_zie: int) -> Dict:
    logger.info('Create unique temp folder for data...')
    run_id = str(uuid.uuid4())
    run_id_dir = os.path.join(temp_storage_path, f'run_{run_id}')

    os.makedirs(run_id_dir)

    logger.info(f'Splitting CSV files in batches of size: {batch_size} rows')
    chunk_iterator = pd.read_csv(raw_data_source_file_path, chunksize=batch_size)
    batch_files = [] # track all files created after splitting csv file

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

def load_batches_into_postgres(meta_data: dict, schema_name: str, table_name: str) -> Dict:
    try:
        if not meta_data:
            msg_exception = f'Metadata is empty {meta_data}'
            logger.info(msg_exception)
            raise ValueError(msg_exception)
    except Exception as err:
        logger.info(f'Metadata is empty: {err}')
        raise
    
    logger.info(f'Pulled metadata')

    # re-construct file path again
    run_dir = os.path.join(temp_storage_path, f"run_{meta_data['run_id']}")
    
    password = get_env('NEON_POSTGRES_PASSWORD')
    user = get_env('NEON_POSTGRES_USER')
    host = get_env('NEON_POSTGRES_HOST')
    port = get_env('NEON_POSTGRES_PORT')
    database = get_env('NEON_POSTGRES_DB_INGEST')

    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

    engine = create_engine(conn_str)

    with engine.connect() as conn:
        try:
            conn.execute('select 1')
            logger.info('Connection was successful...')
        except Exception as err:
            logger.error(f'Error connecting to database: {err}')
            raise

    start_time = time.time()
    logger.info(f"Starting batch loading into Postgres, total batches: {meta_data['total_batches']}")
    total_files_loaded = 0

    for root, dirs, files in os.walk(run_dir):
        for file in files:
            full_file_path = os.path.join(root, file)
            total_files_loaded += 1

    logger.info(f'Total batches loaded into postgres: {total_files_loaded}')

    return meta_data


def clean_up(meta_data: dict) -> Dict:
    try: 
        if not meta_data:
            msg_exception = f'Metadata is empty {meta_data}'
            logger.info(msg_exception)
            raise ValueError(msg_exception)
    except Exception as err:
        logger.info(f'Metadata is empty: {err}')
        raise

    load_run_id = meta_data['run_id']
    batch_files_removed = []
    total_files_removed = 0
    # make sure run id exist in dict
    if load_run_id:
        root_dir = os.path.join(temp_storage_path, f'run_{load_run_id}')

        logger.info(f'Initializing removal of temp files...')
        for root, dirs, files in os.walk(root_dir):
            # go file by file in the dir
            for file in files:
                os.remove(os.path.join(root, file))
                logger.info(f'File {file} removed from {root}')
                total_files_removed += 1
                batch_files_removed.append(file)
            
            os.rmdir(root)
            logger.info(f'Root with run_id run_{load_run_id} removed...')

    metadata_clean = {
        'root dir': f'{temp_storage_path}/run_{load_run_id}',
        'files_removed': sorted(batch_files_removed),
        'total_files_removed': total_files_removed
    }

    return metadata_clean


def refactor_columns_name(raw_data: str) -> pd.DataFrame:
    df = pd.read_csv(raw_data)
    
    # replace non-alpha charactes with lower case
    df.columns = [re.sub(r'\W+', '_', col) for col in df.columns]

    return df
    
columns_names = refactor_columns_name(raw_data=raw_data_source_file_path)

for i in columns_names.columns:
    print(i)

# metadata1 = split_csv_into_batches(raw_data_source_file_path=raw_data_source_file_path, temp_storage_path=temp_storage_path, batch_zie=batch_size)
# metadata_load = load_batches_into_postgres(meta_data=metadata1, schema_name=schema_name, table_name=table_name)
# metadata_clean = clean_up(meta_data=metadata_load)

# print(json.dumps(metadata_clean, indent=4))

