import psycopg2
from sqlalchemy import create_engine

# first connect to database

default_args = {
    "database": '',
    "host": "",
    "user": "",
    "password": "",
    "port": "5432"
}

# conn = psycopg2.connect(database='ingest',
#                         host='',
#                         user='',
#                         password='',
#                         port='5432')

# cursor = conn.cursor()
# sql_cmd = '''
#     select 
#         *
#     from ingest.water_quality_data.station;
# '''

# cursor.execute(query=sql_cmd)
# print(cursor.fetchall())


conn = f'postgresql+psycopg2://{default_args["user"]}:{default_args["password"]}@{default_args["host"]}/{default_args["database"]}'

# Create engine with SSL requirements as connect_args
engine = create_engine(
    conn,
    connect_args={
        'sslmode': 'require'
    }
)

print(engine.table_names())