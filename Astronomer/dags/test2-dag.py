from datetime import datetime

import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from snowflake_plugin.operators.snowflake_operator import SnowflakeOperator

from test2_queries import COPY_INTO, CREATE_STAGE, CREATE_TABLE, TRANSFORMS, parse_s3_config


S3_BUCKET = 'bmez-astronomer'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


def upload_to_s3(**kwargs):
    """
    Generates a CSV that is then uploaded to S3 using the S3Hook. In this case boto reads the AWS credentials
    directly from the boto.cfg file.
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

    stg_query = CREATE_STAGE % (stage_name, bucket_name, access_key_id, secret_key)

    print(stg_query)

    create_stage_task = SnowflakeOperator(
        task_id='create_snowflake_stage',
        query=stg_query,
        snowflake_conn_id='snowflake'
    )

    tbl_query = CREATE_TABLE % table_name

    print(tbl_query)

    create_table_task = SnowflakeOperator(
        task_id='create_snowflake_table',
        query=tbl_query,
        snowflake_conn_id='snowflake'
    )

    ingest_datas = []
    for i in range(3):
        bucket_key = 'test_data_{}.csv'.format(i)

        query = COPY_INTO % (table_name, stage_name, 'csv')

        print(query)

        ingest_datas.append(SnowflakeOperator(
            task_id='s3_to_snowflake_{}'.format(i),
            query=query,
            snowflake_conn_id='snowflake'
        ))

    dummy_start >> upload_datas >> create_stage_task >> create_table_task >> ingest_datas

    dummy_wait = DummyOperator(task_id='wait_for_ingest')
    ingest_datas >> dummy_wait

    for query in TRANSFORMS:
        query_task = SnowflakeOperator(
            task_id='calculate_{0}'.format(query['name']),
            query=query['query'],
            snowflake_conn_id='snowflake',
            )

        dummy_wait >> query_task
