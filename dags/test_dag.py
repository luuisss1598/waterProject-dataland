from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extractors.open_weather_api import extract_data
from transformations.open_weather_trasnform import transform_data
from loaders.open_weather_data_load import load_data

# define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'open_weather_etl',
    default_args=default_args,
    description='An ETL for testing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['open_weather', 'etl'],
)

# we need to wrap function in order to pass data throuhh using _xcom_push adn _xcom_pull
def extract_task(**kwargs):
    print('starting task')
    data = extract_data()
    print('Extraction complete.')
    # pass data to the next task
    kwargs['ti'].xcom_push(key='weather_data', value=data)
    return 'Extraction completed successfully!'

def load_task(**kwargs):
    print('starting loading process...')
    # get the data from previous task
    ti = kwargs['ti']
    # transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_weather_data')
    result = load_data()
    print('data loading complete.')
    return 'Loading completed successfully!'

# Create the tasks
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

# # Set task dependencies (order of execution)
extract >> load;