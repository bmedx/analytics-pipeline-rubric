import configparser
from datetime import datetime

import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from snowflake_plugin.operators.snowflake_operator import SnowflakeOperator


S3_BUCKET = 'bmez-astronomer'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


def parse_s3_config(config_file_name):
    config = configparser.ConfigParser()
    if config.read(config_file_name):
        sections = config.sections()
    else:
        raise AirflowException("Couldn't read {0}".format(config_file_name))

    cred_section = 'Credentials'

    # Option names
    key_id_option = 'aws_access_key_id'
    secret_key_option = 'aws_secret_access_key'

    # Actual Parsing
    if cred_section not in sections:
        raise AirflowException("This config file format is not recognized")
    else:
        try:
            access_key = config.get(cred_section, key_id_option)
            secret_key = config.get(cred_section, secret_key_option)
        except:
            print("Option Error in parsing s3 config file")
            raise
    return access_key, secret_key


def upload_to_s3(**kwargs):
    """
    Generates a CSV that is then uploaded to Google Cloud Storage using the
    S3Hook.
    This is meant to imitate the first step of a traditional ETL DAG: ingesting
    data from some external source.
    This shows how this can be done with an arbitrary python script.
    """
    i = kwargs['i']
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)),
                      columns=['col_a',
                               'col_b',
                               'col_c',
                               'col_d'])

    filename = 'test_data_{}.csv'.format(i)
    df.to_csv(filename, index=False)

    hook = S3Hook(aws_conn_id='astronomer_test_s3')

    hook.load_file(bucket_name=S3_BUCKET,
                   key=filename,
                   filename=filename,
                   replace=True)


queries = [
    {
        'name': 'sum',
        'query': """
        CREATE OR REPLACE TABLE BMESICK.sum  AS
            SELECT
              sum(COL_A) as a,
              sum(COL_B) as b,
              sum(COL_C) as c,
              sum(COL_D) as d
            FROM
              BMESICK.test_data_ingest;
        """
    },

    {
        'name': 'avg',
        'query': """
        CREATE OR REPLACE TABLE BMESICK.avg  AS
            SELECT
              AVG(COL_A) as a,
              AVG(COL_B) as b,
              AVG(COL_C) as c,
              AVG(COL_D) as d
            FROM
              BMESICK.test_data_ingest;
        """
    },

    {
        'name': 'std_dev',
        'query': """
        CREATE OR REPLACE TABLE BMESICK.stddev  AS
            SELECT
              STDDEV(COL_A) as a,
              STDDEV(COL_B) as b,
              STDDEV(COL_C) as c,
              STDDEV(COL_D) as d
            FROM
              BMESICK.test_data_ingest;
        """
    },

]


dag = DAG('test2_dag',
          schedule_interval='@daily',
          default_args=default_args,
          catchup=False)

with dag:
    dummy_start = DummyOperator(task_id='kickoff_dag')

    upload_datas = []
    for i in range(3):
        upload_datas.append(PythonOperator(
            op_kwargs={'i': i},
            task_id='upload_data_{}'.format(i),
            python_callable=upload_to_s3,
            provide_context=True
        ))

    access_key_id, secret_key = parse_s3_config('/etc/boto.cfg')
    table_name = 'BMESICK.test_data_ingest'
    bucket_name = S3_BUCKET
    stage_name = table_name + '_stg'

    stg_query = """
    CREATE OR REPLACE STAGE
    %s
    url='s3://%s/'
    credentials = (aws_key_id = '%s' aws_secret_key = '%s');
    """ % (stage_name, bucket_name, access_key_id, secret_key)

    print(stg_query)

    create_stage_task = SnowflakeOperator(
        task_id='create_snowflake_stage',
        query=stg_query,
        snowflake_conn_id='snowflake'
    )

    tbl_query = """
        CREATE OR REPLACE TABLE %s (
        COL_A number,
        COL_B number,
        COL_C number,
        COL_D number
    )""" % table_name

    print(tbl_query)

    create_table_task = SnowflakeOperator(
        task_id='create_snowflake_table',
        query=tbl_query,
        snowflake_conn_id='snowflake'
    )

    ingest_datas = []
    for i in range(3):
        bucket_key = 'test_data_{}.csv'.format(i)

        query = """
        COPY INTO %s
        FROM @%s
        FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1)
        """ % (table_name, stage_name)

        print(query)

        ingest_datas.append(SnowflakeOperator(
            task_id='s3_to_snowflake_{}'.format(i),
            query=query,
            snowflake_conn_id='snowflake'
        ))

    dummy_start >> upload_datas >> create_stage_task >> create_table_task >> ingest_datas

    dummy_wait = DummyOperator(task_id='wait_for_ingest')
    ingest_datas >> dummy_wait

    for query in queries:
        query_task = SnowflakeOperator(
            task_id='calculate_{0}'.format(query['name']),
            query=query['query'],
            snowflake_conn_id='snowflake',
            )

        dummy_wait >> query_task
