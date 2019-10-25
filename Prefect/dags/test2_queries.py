COPY_INTO = """
COPY INTO %s
FROM @%s
FILE_FORMAT = (type = %s field_delimiter = ',' skip_header = 1)
"""

CREATE_STAGE = """
CREATE OR REPLACE STAGE
%s
url='s3://%s/'
credentials = (aws_key_id = '%s' aws_secret_key = '%s');
"""

CREATE_TABLE = """
    CREATE OR REPLACE TABLE %s (
    COL_A number,
    COL_B number,
    COL_C number,
    COL_D number
)"""

TRANSFORMS = [
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
