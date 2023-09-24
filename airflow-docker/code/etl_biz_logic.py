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