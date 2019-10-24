import os
from prefect import task, Flow

command = 'sleep $[ ( $RANDOM % 3 )  + 1 ]s; echo `date`'


@task
def print_date(name):
    print(name)
    print(os.popen(command).read())
    return name


@task
def print_date_parent(child_name):
    print("Just showing how parameterized task flows usually work.")
    print("Child passed in: '{}'".format(child_name))


with Flow("Test 1") as flow:
    t1 = print_date(name='print_1')
    t2 = print_date(name='print_2')
    t3 = print_date(name='print_3')
    t4 = print_date(name='print_4')
    t5 = print_date(name='print_5')
    t6 = print_date(name='print_6')
    t7 = print_date(name='print_7')
    t8 = print_date(name='print_8')
    t9 = print_date(name='print_9')
    t10 = print_date(name='print_10')
    t11 = print_date(name='print_11')
    t12 = print_date(name='print_12')

    t2.set_upstream(t1)

    t3.set_upstream(t2)

    t4.set_upstream(t3)
    t5.set_upstream(t3)
    t6.set_upstream(t3)
    t7.set_upstream(t3)
    t8.set_upstream(t3)

    t9.set_upstream(t8)
    t10.set_upstream(t8)
    t11.set_upstream(t8)
    t12.set_upstream(t8)

    # Creates a graphviz image of the DAG
    # flow.visualize()

with Flow("Test 1b") as flowb:
    print_date_parent(print_date('print_1'))
    flowb.visualize()


flow.run()
# flowb.run()
