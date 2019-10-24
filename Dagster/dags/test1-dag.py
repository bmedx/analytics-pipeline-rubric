import os
from dagster import pipeline, solid

command = 'sleep $[ ( $RANDOM % 3 )  + 1 ]s; echo `date`'


@solid
def sleep_and_print_date(_):
    print(os.popen(command).read())


@pipeline
def test1_pipeline():
    # Each re-use of the same solid in a DAG needs its own alias. Make and run the 12 unique solids here.
    for i in range(1, 13):
        alias = 'print_{}'.format(i)
        sleep_and_print_date.alias(alias)()


"""
The DAG would normally be assembled by stacking function calls like the one below, but Dagster is smart enough to know 
that these inputs / outputs make no sense and won't allow it, forcing us to do the right thing and actually execute them 
all in parallel (at least on Airflow, locally they will run sequentially). 

@pipeline
def test1_pipeline():
    _solid('print_1')(
        _solid('print_2')(
            _solid('print_3')(
                _solid('print_4')(),
                _solid('print_5')(),
                _solid('print_6')(),
                _solid('print_7')(),
                _solid('print_8')(
                    _solid('print_9')(),
                    _solid('print_10')(),
                    _solid('print_11')(),
                    _solid('print_12')()
                )
            )
        )
    )
"""
