from sqlalchemy import create_engine
from utils.load_variables import get_env
from utils.logging_config import logger


class PostgreConnection:
    def __init__(self):
        self.user = get_env('NEON_POSTGRES_USER')
        self.password = get_env('NEON_POSTGRES_PASSWORD')
        self.host = get_env('NEON_POSTGRES_HOST')
        self.port = get_env('NEON_POSTGRES_PORT')
        self.database = get_env('NEON_POSTGRES_DB_INGEST')
    
    def get_engine_connection(self):
        db_connection = f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'

        engine = create_engine(db_connection, connect_args = {'sslmode': 'require'})

        return engine
    
# connect_to_postgres = PostgreConnection()