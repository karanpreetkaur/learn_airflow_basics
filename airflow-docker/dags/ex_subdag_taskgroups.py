from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.utils.task_group import TaskGroup

with DAG("ex_task_groups", 
         default_args = {
                        "owner":"airflow", 
                         "start_date": datetime(2022, 1, 1)
                         }) as dag:
    start = DummyOperator(task_id="start")
    # a = DummyOperator(task_id="Task_a")
    # a1 = DummyOperator(task_id="Task_a1")
    # b = DummyOperator(task_id="Task_b1")
    # c = DummyOperator(task_id="Task_c")
    d = DummyOperator(task_id="Task_d")
    e = DummyOperator(task_id="Task_e")
    f = DummyOperator(task_id="Task_f")
    g = DummyOperator(task_id="Task_g")
    end = DummyOperator(task_id="end")

# start >> a >> b >> c >> d >> e >> f >> g >> end

    with TaskGroup("A-A1", tooltip="Task Group for A & A1") as grp_1:
        a = DummyOperator(task_id="Task_a")
        a1 = DummyOperator(task_id="Task_a1")
        b = DummyOperator(task_id="Task_b1")
        c = DummyOperator(task_id="Task_c")
        a >> a1


start >> grp_1 >> d >> e >> f >> g >> end