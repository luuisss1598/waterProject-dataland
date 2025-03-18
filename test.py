from sqlalchemy import create_engine
from utils.load_variables import get_env
import pandas as pd
from utils.logging_config import logger

password = get_env('NEON_POSTGRES_PASSWORD')
user = get_env('NEON_POSTGRES_USER')
host = get_env('NEON_POSTGRES_HOST')
port = get_env('NEON_POSTGRES_PORT')
database = get_env('NEON_POSTGRES_DB_INGEST')

schema_name = 'open_weather_api'
table_name = 'temp_open_weather_api_historical_hourly'

conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
engine = create_engine(conn_str)

df = pd.read_csv('./data/raw/open_weather_historical_data.csv')
df = df.head(n=100)

# print(pd.io.sql.get_schema(df, name=table_name, con=engine), index=False)

df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='replace', index=False)
logger.info('Data laoded into table...')