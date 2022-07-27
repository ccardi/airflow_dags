import datetime

from airflow import models
from airflow.operators import bash_operator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
}

def run_python_script(task_id, path):
    temp= bash_operator.BashOperator(
        task_id=task_id,
        # This example runs a Python script from the data folder to prevent
        # Airflow from attempting to parse the script as a DAG.
        bash_command='python '+path,
    )
    return  temp
    
with models.DAG(
        'dag_python_script',
        schedule_interval=datetime.timedelta(minutes=59),
        default_args=default_dag_args) as dag:
    
    run_python = run_python_script('run_python_start', '/home/airflow/gcs/dags/scripts/python_script2.py')
    run_python2 = run_python_script('run_python_secondstep', '/home/airflow/gcs/dags/scripts/python_script2.py')
    run_python >> run_python2
    for x in range(6):
        run_python3 = run_python_script('run_python_'+str(x),'/home/airflow/gcs/dags/scripts/python_script2.py')
        run_python2 >> run_python3
        
