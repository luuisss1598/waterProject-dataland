from sqlalchemy import create_engine
from utils.logging_config import logger
from utils.load_variables import get_env
from typing import Dict
import pandas as pd
import os
import time
import re

"""Function to load batched data into Postgres database"""

def load_batches_into_postgres(meta_data: dict, temp_storage_path:str, schema_name: str, table_name: str) -> Dict:
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
            df = pd.read_csv(full_file_path) # read batched data into dataframe

            try:
                df.columns = [re.sub(r'\W+', '_', col) for col in df.columns] # clean columns
                df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False)
                logger.info(f'{len(df)} rows ingested from batch {file} into table {table_name}')
            except Exception as err:
                logger.info(f'Failed to load data to Postgres: {err}')
                raise
            
            total_files_loaded += 1
    
    end_time = time.time()
    total_elpased_time = (end_time-start_time)
    
    logger.info(f'Total batches loaded into postgres: {total_files_loaded}')
    logger.info(f'Total time to ingest data: {total_elpased_time}')
    meta_data['time_ingesting_data'] = total_elpased_time
    
    return meta_data