import numpy as np
import pandas as pd
from dagster import (
    InputDefinition,
    OutputDefinition,
    ModeDefinition,
    Output,
    PresetDefinition,
    file_relative_path,
    lambda_solid,
    pipeline,
    solid,
)
from dagster_aws.s3.file_manager import S3FileHandle
from dagster_aws.s3.resources import s3_resource
from dagster_aws.s3.solids import file_handle_to_s3
from dagster_aws.s3.system_storage import s3_plus_default_storage_defs
from dagster_snowflake import snowflake_resource
from dagster_pandas import DataFrame

from test2_queries import COPY_INTO, CREATE_STAGE, CREATE_TABLE, TRANSFORMS, parse_s3_config

# Typically we would get these from some other configuration source that we could create on a per-env basis
access_key_id, secret_key = parse_s3_config('test_creds/boto.cfg')
table_name = 'BMESICK.test_data_ingest_dagster'
stage_name = table_name + '_stg'
bucket_name = 'bmez-astronomer'

# Modes allow you to configure substantial behavior based on environment
# (use local disk instead of S3 for local runs, sqlite instead of Snowflake, etc)
prod_mode = ModeDefinition(
    name='prod',
    resource_defs={
        's3': s3_resource,
        'snowflake': snowflake_resource
    },
    system_storage_defs=s3_plus_default_storage_defs,
)

# Presets are a type of configuration where each file can overwrite values in the next, this is where per-environment
# settings and, I guess, secrets go?
preset_defs = PresetDefinition.from_files(
    name='prod',
    mode='prod',
    environment_files=[
        file_relative_path(__file__, 'environments/shared.yaml'),
    ],
)


# Solids are the units of execution that are steps in a pipeline. Lambda solids are simpler and do not require
# configuration or context.
@lambda_solid(
    output_def=OutputDefinition(DataFrame),  # Describes what this solid will return
)
def generate_test_data():
    """
    Generate random data
    """
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)),
                      columns=['col_a',
                               'col_b',
                               'col_c',
                               'col_d'])

    return DataFrame(df)


@lambda_solid(
    input_defs=[InputDefinition('data', DataFrame)],  # Describes inputs to this solid
    # output_def=OutputDefinition(S3FileHandle)  # Not working for reasons I don't yet understand
)
def upload_data_to_s3(data):
    """
    Uploads our randomly generated dataframe to S3 and returns the S3 file handle
    """
    filename = 'foo.csv'
    data.to_csv(filename, index=False)

    with open(filename, 'rb') as fh:
        materialization, s3_handle = file_handle_to_s3(fh)

    yield s3_handle


@solid(required_resource_keys={'snowflake'})
def snowflake_setup(context):
    """
    Makes sure our Snowflake stage and table exist
    """
    context.resources.snowflake.execute_query(CREATE_STAGE % (stage_name, bucket_name, access_key_id, secret_key))
    context.resources.snowflake.execute_query(CREATE_TABLE % table_name)
    yield Output(value=table_name, output_name='result')


@solid(required_resource_keys={'snowflake'})
def snowflake_load(context, setup_output, s3_file_handle):
    """
    Executes the S3 load into Snowflake. The Snowflake stage actually will read all csv files in the bucket, but we
    could just grab the file from s3_file_handle. This could also accept, say, a table name from snowflake_setup if
    it was automatically generated. Right now we don't use the inputs.
    """
    context.resources.snowflake.execute_query(COPY_INTO % (table_name, stage_name, 'csv'))
    yield Output(value=table_name, output_name='result')


@solid(required_resource_keys={'snowflake'})
def snowflake_transform(context, copy_success):
    """
    Execute each of the SQL transforms in Snowflake. In theory this should start 3 tasks in Airflow deployment but I
    haven't been able to test it. If not, we could have it aliased 3 times and accept the transform name as a param to
    accomplish the same thing.
    """
    for t in TRANSFORMS:
        context.resources.snowflake.execute_query(t['query'])


@pipeline(mode_defs=[prod_mode], preset_defs=[preset_defs])
def test2_pipeline():
    """
    This is the pipeline definition, it's just a sequence of Python calls.

    We could parameterize upload_data_to_s3 to accept several different generate_test_data calls to add more files
    like we do in the Airflow version of this test, I just ran out of time.
    """
    snowflake_transform(
        snowflake_load(
            snowflake_setup(),
            upload_data_to_s3(
                generate_test_data()
            )
        )
    )