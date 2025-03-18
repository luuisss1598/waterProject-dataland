import pandas as pd
from utils.postgres_connection import PostgreConnection
from extractors.extract_open_weather_data import OpenWeatherAPI
from transformations.open_weather_transform import transform_open_weather_data_from_api
from utils.logging_config import logger
from sqlalchemy import create_engine, text
from utils.load_variables import get_env
import json

def load_open_weather_api_5min_incremental(data_to_load, table_name: str, schema_name: str):
    password = get_env('NEON_POSTGRES_PASSWORD')
    user = get_env('NEON_POSTGRES_USER')
    host = get_env('NEON_POSTGRES_HOST')
    port = get_env('NEON_POSTGRES_PORT')
    database = get_env('NEON_POSTGRES_DB_INGEST')

    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    
    # start engine for sqlalchemy
    engine = create_engine(conn_str)

    sql_cmd = f'''
        select max(extracted_at)
        from {schema_name}.{table_name};
    '''

    with engine.connect() as conn:
        result = conn.execute(text(sql_cmd))

        # get latest timestamp ingested, if empty then is None
        last_timestamp = result.scalar()    
    
    # i need to modify dataframe in order to comply with DDL created in postgres
    data_to_load = pd.DataFrame(data_to_load)
    data_to_load['coord'] = data_to_load['coord'].apply(json.dumps)
    data_to_load['weather'] = data_to_load['weather'].apply(json.dumps)
    data_to_load['main'] = data_to_load['main'].apply(json.dumps)
    data_to_load['wind'] = data_to_load['wind'].apply(json.dumps)
    data_to_load['clouds'] = data_to_load['clouds'].apply(json.dumps)
    data_to_load['sys'] = data_to_load['sys'].apply(json.dumps)
    data_to_load['extracted_at'] = pd.to_datetime(data_to_load['extracted_at'])

    # we need to filter only new rows
    if last_timestamp:
        data_to_load = data_to_load[data_to_load['extracted_at'] > last_timestamp]

    if not data_to_load.empty:
        data_to_load.to_sql(
            name=table_name,
            schema=schema_name,
            con=engine,
            if_exists='append',
            index=False
        )
        logger.info(f'Inserted {len(data_to_load)} new rows into {schema_name}.{table_name}')
    else:
        logger.info('No new data to insert.')
