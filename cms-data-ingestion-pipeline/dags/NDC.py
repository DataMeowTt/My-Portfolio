from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.load_and_unzip import download_and_unzip

default_args = {
    'owner': 'me',
    'start_date': datetime(2025, 11, 14),
    'retries': 1,
}

with DAG(
    dag_id='NDC_data_download',
    default_args=default_args,
    description='Download NDC data from open.fda.gov',
    schedule="@monthly"
) as dag:
    start = EmptyOperator(task_id='start')


    download_task = PythonOperator(
        task_id = 'download_zip_file',
        python_callable = download_and_unzip
    )

    end = EmptyOperator(task_id='end')

    start >> download_task >> end