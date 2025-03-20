# from sqlalchemy import create_engine
# from utils.load_variables import get_env
# import pandas as pd
# from utils.logging_config import logger

# password = get_env('NEON_POSTGRES_PASSWORD')
# user = get_env('NEON_POSTGRES_USER')
# host = get_env('NEON_POSTGRES_HOST')
# port = get_env('NEON_POSTGRES_PORT')
# database = get_env('NEON_POSTGRES_DB_INGEST')

# schema_name = 'open_weather_api'
# table_name = 'temp_open_weather_api_historical_hourly'

# conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
# engine = create_engine(conn_str)

# df = pd.read_csv('./data/raw/open_weather_historical_data.csv')
# df = df.head(n=100)

# # print(pd.io.sql.get_schema(df, name=table_name, con=engine), index=False)

# df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='replace', index=False)
# logger.info('Data laoded into table...')

from utils.logging_config import logger
import json
from utils.load_variables import get_env
from sqlalchemy import create_engine
import os
import pandas as pd
import time

temp_storage_path = './data/tmp/airflow'
schema_name = 'open_weather_api'
table_name = 'temp_open_weather_api_historical_hourly'

def load_batches_to_postgres(**context):
    try: 
        # meta_data =  context['ti'].xcom_pull(task_ids='split_csv_file')
        meta_data = {
            "batch_files": {
                0: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0000.csv",
                1: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0001.csv",
                2: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0002.csv",
                3: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0003.csv",
                4: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0004.csv",
                5: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0005.csv",
                6: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0006.csv",
                7: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0007.csv",
                8: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0008.csv",
                9: "./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0009.csv",
                10 :"./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0010.csv",
                11 :"./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0011.csv",
                12 :"./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0012.csv",
                13 :"./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0013.csv",
                14 :"./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0014.csv",
                15 :"./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0015.csv",
                16 :"./data/tmp/airflow/run_4062f7b7-bef5-498a-ab46-8159fa1c936e/batch_0016.csv",
            },
            "run_id":"4062f7b7-bef5-498a-ab46-8159fa1c936e",
            "total_batches": 17
        }
        
        if meta_data is None:
            logger.error(f'Metadata is empty: {json.dumps(meta_data, indent=4)}')
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
    
    return 'Data has been loaded into Postgres DB...'
    
load_batches_to_postgres()