from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

import pandas as pd

# All constants needs to be declared as global variables
global_variable_gv = "K2 Analytics"

def extract_fn():
    print("Logic to Extract Data")
    print("Value of Global Variable", global_variable_gv)
    # rtn_val = "Analytics Training"
    # return rtn_val

    # creating a DataFRame object
    details = {
        'cust_id': [1, 2, 3, 4],
        'Name': ['Rajesh', 'Jakhotia', 'K2', 'Analytics']
    }
    df = pd.DataFrame(details)
    return df

# XCOM - Cross Communication
# ti - Task Instance
def transform_fn(a1, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids = ["EXTRACT"])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    extract_rtn_object = xcom_pull_obj[0]
    print("value of xcom pull object is {}".format(extract_rtn_object))
    print("The value of a1 is", a1)
    print("Logic to Transform Data")
    return 10

def load_fn(p1, p2, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=["EXTRACT"])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    extract_rtn_object = xcom_pull_obj[0]
    print("value of xcom pull object is {}".format(extract_rtn_object))
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to Load Data")

# Default arguments as dict
def_args = {
    "owner":"airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dtime(2022, 6, 15)
}

with DAG("ex_com_push_pull", 
         default_args = def_args,
         catchup=False) as dag:
    start = DummyOperator(task_id="START")

    e = PythonOperator(
        task_id = "EXTRACT",
        python_callable = extract_fn
    )

    # op_args -> Operator arguments
    t = PythonOperator(
        task_id = "TRANSFORM",
        python_callable = transform_fn,
        op_args = ["Learning Data Engineering with Airflow"]
        
    )

    l = PythonOperator(
        task_id = "LOAD",
        python_callable = load_fn,
        # op_args = ["K2", "Analytics"]
        op_kwargs = {"p2":"Analytics", "p1":"K2"}
    )

    end = DummyOperator(task_id="END")

start >> e  >> t >> l >> end