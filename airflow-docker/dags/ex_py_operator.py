from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

def extract_fn():
    print("Logic to Extract Data")
    return "some object"

# XCOM - Cross Communication
def transform_fn(a1):
    print("The value of a1 is", a1)

def load_fn(p1, p2):
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

with DAG("ex_python_operator", 
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