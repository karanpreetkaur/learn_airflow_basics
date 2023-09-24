from datetime import datetime as dtime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
import time

def get_config_params(**kwargs):
    context = get_current_context()
    print(f"context value", {context})
    logical_date = kwargs["logical_date"]
    custom_param = kwargs['dag_run'].conf.get('custom_parameter')
    todays_date = dtime.now().date()

    if logical_date.date() == todays_date:
        print("Normal Execution")
    else:
        print("Back-dated Execution")
        if custom_param is not None:
            print("Custom parameter is: {custom_param}")
    time.sleep(15)

default_args = {"owner":"airflow", "retries":0, "start_date": dtime(2021, 1, 1)}
with DAG("ex_dag_config_params", 
         default_args=default_args, 
         catchup=False) as dag:
    start = DummyOperator(task_id="Start")
    config_params = PythonOperator(task_id="DAG_CONFIG_PARAMS", python_callable = get_config_params)
    end = DummyOperator(task_id="End")

start >> config_params >> end