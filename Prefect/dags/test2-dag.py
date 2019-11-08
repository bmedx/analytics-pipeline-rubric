import numpy as np
import pandas as pd
from prefect import Flow, config, task
from prefect.client import Secret
from prefect.tasks.aws.s3 import S3Upload
# This has an upstream issue, fixed locally in snowflake_task
# from prefect.tasks.snowflake.snowflake import SnowflakeQuery

from test2_queries import COPY_INTO, CREATE_STAGE, CREATE_TABLE, TRANSFORMS
from snowflake_task import SnowflakeQuery


def get_snowflake_creds():
    sf_config = Secret("SNOWFLAKE").get()
    return {
        'account': sf_config.ACCOUNT,
        'user': sf_config.USER,
        'password': sf_config.PASSWORD,
        'database': sf_config.DATABASE,
        'schema': sf_config.SCHEMA,
        'role': sf_config.ROLE,
        'warehouse': sf_config.WAREHOUSE,
    }


@task
def generate_test_data():
    """
    Generates a randomized pandas dataframe to be later uploaded to S3
    """
    return pd.DataFrame(
        np.random.randint(0, 100, size=(100, 4)),
        columns=[
            'col_a',
            'col_b',
            'col_c',
            'col_d'
        ]
    )


@task
def prep_data_for_s3(dataframe):
    dataframe.to_csv('tmp.csv', index=False)

    with open('tmp.csv', 'r') as f:
        data_str = f.read()

    return data_str


@task
def snowflake_setup():
    """
    Makes sure our Snowflake stage and table exist
    """
    s3_config = Secret("AWS_CREDENTIALS").get()

    kwargs = get_snowflake_creds()
    kwargs['query'] = CREATE_STAGE % (
        config.test2_config.STAGE_NAME,
        config.test2_config.BUCKET_NAME,
        s3_config.ACCESS_KEY,
        s3_config.SECRET_ACCESS_KEY
        )

    SnowflakeQuery(**kwargs)(flow=flow)

    kwargs['query'] = CREATE_TABLE % config.test2_config.TABLE_NAME
    SnowflakeQuery(**kwargs)(flow=flow)


@task
def snowflake_load():
    """
    Executes the S3 load into Snowflake. The Snowflake stage actually will read all csv files in the bucket, but we
    could just grab the file from s3_file_handle. This could also accept, say, a table name from snowflake_setup if
    it was automatically generated. Right now we don't use the inputs.
    """
    kwargs = get_snowflake_creds()
    kwargs['query'] = COPY_INTO % (config.test2_config.TABLE_NAME, config.test2_config.STAGE_NAME, 'csv')
    SnowflakeQuery(**kwargs)(flow=flow)


@task
def snowflake_transform():
    """
    Execute each of the SQL transforms in Snowflake. In theory this should start 3 tasks in Airflow deployment but I
    haven't been able to test it. If not, we could have it aliased 3 times and accept the transform name as a param to
    accomplish the same thing.
    """
    kwargs = get_snowflake_creds()

    for t in TRANSFORMS:
        kwargs['query'] = t['query']
        SnowflakeQuery(**kwargs)(flow=flow)


with Flow("Test 2") as flow:
    test_data = generate_test_data()
    data_str = prep_data_for_s3(test_data)
    s3_upload = S3Upload()(data_str, key='prefect/data1.csv', bucket='bmez-astronomer')

    setup = snowflake_setup()
    load = snowflake_load()
    transform = snowflake_transform()

    load.set_upstream((setup, s3_upload))
    transform.set_upstream(load)

    flow.visualize()

flow.run()
