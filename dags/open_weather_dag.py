from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.logging_config import logger
import pandas as pd

from extractors.extract_open_weather_data import OpenWeatherAPI
from transformations.open_weather_transform import transform_open_weather_data_from_api
from loaders.open_weather_data_load import load_open_weather_api_5min_incremental

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": "lherrera5034@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)  # Fixed typo here (removed the line break)
}

# create instance of open weather api to extract data from api
extract_data_obj = OpenWeatherAPI()
zip_code = '94804'
schema_name = 'open_weather_api'
table_name = 'open_weather_api_5min'

"""
In orde to use XCOM, I need to pass the data through each task as json. Therefore, I need to manipulate the data each time.
"""

with DAG(
    'open_weather_etl',
    default_args=default_args,
    description='A dag to extract data from OpenWeatherAPI every 5 minutes',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 3, 3),
    catchup=False,
    tags=['weather']
) as dag:

    def extract_task(**context):
        logger.info('Starting extraction...')
        data = extract_data_obj.get_daily_open_weather_data(zip_code=zip_code)

        # push data into xcom for next tasks
        context['ti'].xcom_push(key='open_weather_api_raw_data', value=data)
        logger.info('Extraction complete.')
        return "Extraction successful"

    def transform_task(**context):
        logger.info('Starting transformation...')
        raw_data = context['ti'].xcom_pull(task_ids='extract', key='open_weather_api_raw_data')

        # pass raw data into tranformation
        transformed_data = transform_open_weather_data_from_api(raw_data)
        context['ti'].xcom_push(key='open_weather_transformed_data', value=transformed_data.to_dict(orient='records'))

        logger.info('Transformation complete.')
        return "Transformation successful"

    def load_task(**context):
        logger.info('Loading data into Postgres...')
        dict_data = context['ti'].xcom_pull(task_ids='transform', key='open_weather_transformed_data')

        # push data in order to see how it looks and debug
        context['ti'].xcom_push(key='loaded_data_example', value=dict_data)
        
        load_open_weather_api_5min_incremental(
            data_to_load=dict_data, 
            table_name=table_name, 
            schema_name=schema_name
        )
        
        logger.info('Data load complete.')
        return "Load successful"

    task_1 = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )

    task_2 = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
    )

    task_3 = PythonOperator(
        task_id='load',
        python_callable=load_task,
    )

    task_1 >> task_2 >> task_3  