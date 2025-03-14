import pandas as pd 
from dotenv import load_dotenv, find_dotenv
import os 

find_path = find_dotenv()
load_dotenv(find_path)

def extract_data(): 
    df = pd.read_csv(filepath_or_buffer='./data/raw/station_5.csv')
    neon_postgres_host = os.getenv('NEON_POSTGRES_HOST')
    neon_postgres_port = os.getenv('NEON_POSTGRES_PORT')
    neon_postgres_user = os.getenv('NEON_POSTGRES_USER')
    neon_postgres_password = os.getenv('NEON_POSTGRES_PASSWORD')

    print(neon_postgres_host)
    print(neon_postgres_port)
    print(neon_postgres_user)
    print(neon_postgres_password)


extract_data()