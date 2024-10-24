from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def set_stage():
    cur = return_snowflake_conn()

    set_stage_sql = """ CREATE OR REPLACE STAGE dev.raw_data.blob_stage
                    url = 's3://s3-geospatial/readonly/'
                    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
                    """

    cur.execute(set_stage_sql)


@task
def load():
    cur = return_snowflake_conn()

    load_sql_1 = """
    COPY INTO dev.raw_data.user_session_channel
    FROM @dev.raw_data.blob_stage/user_session_channel.csv;
    """
    cur.execute(load_sql_1)

    load_sql_2 = """
    COPY INTO dev.raw_data.session_timestamp
    FROM @dev.raw_data.blob_stage/session_timestamp.csv;
    """
    cur.execute(load_sql_2)


with DAG(
    dag_id='SessionToSnowflake',
    start_date = datetime(2024,10,22),
    catchup=False,
    tags=['Snowflake', 'ELT'],
    schedule = '45 3 * * *'
) as dag:

    set_stage() >> load()
