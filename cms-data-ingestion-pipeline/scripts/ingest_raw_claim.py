import calendar
from datetime import datetime, date
import os
import re
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from db import get_cursor

load_dotenv()
FILE_PATH = os.getenv("FILE_PATH")
file_name = os.path.basename(FILE_PATH)

def determine_sql_script(file_name):
    file_name = os.path.basename(file_name)
    print(f"Processing file: {file_name}")

    query = """
        SELECT sql_file_name
        FROM customer_sql_mapping
        WHERE %s LIKE ('%%' || csv_file_name || '%%')
        LIMIT 1;
        """

    with get_cursor() as cursor:
        print(f"Executing query: {query.replace('%s', f'\'{file_name}\'')}")
        cursor.execute(query, (file_name,))
        result = cursor.fetchone()

        if not result:
            raise ValueError(f"No SQL script mapping found for file: {file_name}")

        script_name = result["sql_file_name"]

    return {"script_name": script_name, "file_name": file_name}


def load_sql_file(**kwargs):
    script_name = kwargs["ti"].xcom_pull(task_ids="determine_sql_script")["script_name"]
    file_path = f"/opt/airflow/dags/sql/{script_name}"

    try:
        with open(file_path, "r") as sql_file:
            sql_content = sql_file.read()
            return sql_content
    except FileNotFoundError:
        raise ValueError(f"SQL file not found: {file_path}")


def check_date_quality_and_validate(**kwargs):
    file_name = kwargs["params"]["file_name"]

    match = re.search(r"_(\d{8})\.csv$", file_name)

    if not match:
        raise AirflowException(
            f"Invalid file name format: {file_name}. Expected a date suffix in YYYYMMDD format."
        )

    expected_date = datetime.strptime(match.group(1), "%Y%m%d").date()
    # Get the year and month from the date
    year, month = expected_date.year, expected_date.month

    # Get the last day of the month
    last_day = calendar.monthrange(year, month)[1]

    # Construct the last date of the month
    last_date_of_month = date(year, month, last_day)
    # Query Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    sql = f"""
    SELECT
        MIN(claim_start_date) AS MIN_DATE,
        MAX(claim_start_date) AS MAX_DATE
    FROM
        raw_claims
    WHERE
        source_file_name = '{file_name}';
    """
    result = snowflake_hook.get_first(sql)

    if not result:
        raise AirflowException("Date range query did not return any results.")

    min_date, max_date = result

    # Validate that min_date and max_date match the expected date
    if min_date != expected_date or max_date != last_date_of_month:
        raise AirflowException(
            f"Data quality check failed: Expected dates ({expected_date}) do not match the range in the table (MIN_DATE: {min_date}, MAX_DATE: {max_date})."
        )

    print(f"Data quality check passed: MIN_DATE = {min_date}, MAX_DATE = {max_date}")