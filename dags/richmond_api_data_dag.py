from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": "lherrera5034@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)  # Fixed typo here (removed the line break)
}

with DAG(
    'ingest_richmond_api_data',
    default_args=default_args,
    description='Data pipeline to ingest data from Richmond data catalog, multiple endpoints.',
    schedule_interval=None,
    start_date=datetime(2025, 3, 24),
    catchup=False,
    tags=['water', 'api']
) as dag:
    
    task_1 = EmptyOperator(
        task_id='extract_endpoint_1',
    )

    task_2 = EmptyOperator(
        task_id='extract_endpoint_2'
    )

    task_3 = EmptyOperator(
        task_id='extract_endpoint_3',
    )

    task_4 = EmptyOperator(
        task_id='transform_all_data',
    )

    task_5 = EmptyOperator(
        task_id='load_endpoint_1',
    )
    
    task_6 = EmptyOperator(
        task_id='load_endpoint_2',
    )

    task_7 = EmptyOperator(
        task_id='load_endpoint_3',
    )

    task_8 = EmptyOperator(
        task_id='clean_up_all_files',
    )


    [task_1, task_2, task_3] >> task_4 >> [task_5, task_6, task_7] >> task_8