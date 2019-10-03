# analytics-pipeline-rubric

Files to support testing different analytics pipelines providers. Each provider will have a folder with these tests:

## Test 1

A simple pipeline that just prints the date in a broad DAG.

## Test 2

A fairly typical pipeline that:

- Exports a large dataset from a publicly available MySQL database to S3
- Loads the tables in parallel from S3 to Snowflake
- Runs a set of dbt transforms in Snowflake
