import psycopg2
from utils.logging_config import logger


class PostgresConnection:
    def __init__(self, database: str, host: str, user: str, password: str, port: str = 5432):
        # create parameters for connection with psycopg2
        self.connection_params = {
            'database': database,
            'host': host,
            'user': user,
            'password': password,
            'port': port
        }
        
        # test to check if conneciton is alive or not
        self.connection = None
        logger.info(f'Initilizing connection for database {database} on host {host}')
        
    def connect(self):
        """Establish initial connection to the database""" 
        if self.connection is None or self.connection.closed:
            try:
                self.connection = psycopg2.connect(**self.connection_params)
                self.connection.autocommit = False
                logger.info('Successfully connected to database...')
            except Exception as error:
                logger.error(f'Error connecting to database: {error}...')
    
    def execute_query(self):
        pass 
    
    
database = ''
host = ''
user = ''
password = ''
port = '5432'

# connection_params = {
#     'database': database,
#     'host': host,
#     'user': user,
#     'password': password,
#     'port': port
# }

# print(**connection_params)

conn = PostgresConnection(database=database, host=host, user=user, password=password, port=port)

conn.connect()