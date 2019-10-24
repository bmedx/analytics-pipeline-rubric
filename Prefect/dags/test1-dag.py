from prefect import task, Flow

command = 'sleep $[ ( $RANDOM % 3 )  + 1 ]s; echo `date`'


@task
def print_date():
    pass


with Flow("Test 1") as flow:
    print_date()


flow.run()
