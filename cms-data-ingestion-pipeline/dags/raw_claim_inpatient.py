from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import os
from scripts .ingest_raw_claim import (
    determine_sql_script,
    load_sql_file,
    check_date_quality_and_validate
)
from dotenv import load_dotenv

load_dotenv()

# pass via event bridge
FILE_PATH = os.getenv("FILE_PATH")
file_name = os.path.basename(FILE_PATH)

with DAG(
    dag_id="raw_claims",
    start_date=datetime(2024, 11, 11),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1},
) as dag:
    start = EmptyOperator(task_id="start")

    # Determine the SQL script to execute
    determine_script_task = PythonOperator(
        task_id="determine_sql_script",
        python_callable=determine_sql_script,
        provide_context=True,
        op_kwargs={"file_name": FILE_PATH},
    )

    # Upload the CSV file to a Snowflake stage
    upload_to_snowflake_stage = SnowflakeOperator(
        task_id="upload_to_snowflake_stage",
        snowflake_conn_id="snowflake_default",
        sql=f"""
        PUT 'file:///{FILE_PATH}'
        @raw_claims_stage;
        """,
    )

    # Delete existing data from raw_claims in Snowflake
    delete_raw_claims = SnowflakeOperator(
        task_id="delete_from_raw_claims",
        snowflake_conn_id="snowflake_default",
        sql=f"""
            DELETE FROM raw_claims
            WHERE source_file_name = '{file_name}';
        """,
    )

    # Task to read the SQL file
    read_sql_file = PythonOperator(
        task_id="read_sql_file",
        python_callable=load_sql_file,
        provide_context=True,
    )

    # Insert new data into raw_claims
    insert_raw_claims = SnowflakeOperator(
        task_id="insert_into_raw_claims",
        snowflake_conn_id="snowflake_default",
        sql="{{ ti.xcom_pull(task_ids='read_sql_file') }}",  # Get SQL content from the Python task
        params={
            "file_name": "{{ ti.xcom_pull(task_ids='determine_sql_script')['file_name'] }}"
        },
    )

    # Task to fetch and validate the date range from Snowflake
    dq_date_range = PythonOperator(
        task_id="validate_date_range",
        python_callable=check_date_quality_and_validate,
        provide_context=True,
        params={"file_name": {file_name}},
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> determine_script_task
        >> upload_to_snowflake_stage
        >> delete_raw_claims
        >> read_sql_file
        >> insert_raw_claims
        >> dq_date_range
        >> end
    )
