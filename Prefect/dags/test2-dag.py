import io
import numpy as np
import pandas as pd
from prefect import task, Flow
from prefect.tasks.aws.s3 import S3Upload
from prefect.tasks.snowflake.snowflake import SnowflakeQuery


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


with Flow("Test 2") as flow:
    test_data = generate_test_data()
    data_str = prep_data_for_s3(test_data)
    s3 = S3Upload()(data_str, key='hey.txt', bucket='bmez-astronomer')
    # flow.visualize()

flow.run()
