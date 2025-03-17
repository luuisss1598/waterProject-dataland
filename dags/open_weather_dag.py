from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.logging_config import logger

from extractors.open_weather_api import extract_data
from transformations.open_weather_transform import transform_data
from loaders.open_weather_data_load import load_data


default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": "lherrera5034@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    'open_weather_etl',
    default_args=default_args,
    description='A dag to extract data from OpenWeatherAPI every 5 minutes',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 3, 3),
    catchup=False,
    tags=['weather']
) as dag:

    def extract_task(**context):
        logger.info('Starting extraction...')
        data = extract_data()
        # context['ti'].xcom_push(key='raw_data', value=data)
        logger.info('Extraction complete.')

    def transform_task(**context):
        logger.info('Starting transformation...')
        # raw_data = context['ti'].xcom_pull(task_ids='extract', key='raw_data')
        transformed_data = transform_data()
        # context['ti'].xcom_push(key='transformed_data', value=transformed_data)
        logger.info('Transformation complete.')

    def load_task(**context):
        logger.info('Loading data into Postgres...')
        # transformed_data = context['ti'].xcom_pull(key='transform', value='transformed_data')
        load_data()
        logger.info('Data load complete.')

    task_1 = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
        provide_context=True,
    )

    task_2 = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
        provide_context=True,
    )

    task_3 = PythonOperator(
        task_id='load',
        python_callable=load_task,
        provide_context=True,
    )

    task_1 >> task_2 >> task_3;