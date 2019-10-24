# analytics-pipeline-rubric

Files to support testing different analytics pipelines providers. Each provider will have a folder with these tests:

## Test 1

A simple pipeline that just prints the date in a broad DAG.

## Test 2

A fairly typical pipeline that:

- Generates a small dataset of 3 csv files based on a pandas dataframe locally
- Pushes the files to an S3 bucket
- Creates or replaces a Snowflake stage for the S3 files
- Creates or replaces a Snowflake table 
- Loads the data files in parallel from S3 to Snowflake
- Runs a set of transforms in parallel in Snowflake, creating new tables
