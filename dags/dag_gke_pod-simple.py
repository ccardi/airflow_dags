from airflow import DAG
from airflow.models import Variable
import logging
from datetime import datetime, date, timedelta
from airflow import models
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

default_args = {
    #dag arg
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 11),
    'email': ['cardi@google.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #GKE Operator global config (**gke_conf)
    'project_id' : 'pod-fr-retail'
    ,'namespace':'default'
    ,'location' :'europe-west1'  #'europe-west1-c'
    ,'cluster_name' : 'autopilot-cluster-2'  #'europe-west1-luxury-compose-f60ca3a5-gke'
    , 'is_delete_operator_pod': 'False'
    #,'image_pull_policy' :  'Always'
}


# Define the pipeline schedule
schedule_interval = None
with DAG("dag_gke_pod-simple", default_args=default_args, catchup=False, schedule_interval=schedule_interval) as dag:

    cmds= ['python' ,'/pandas_to_bq.py']
    pandas_task = GKEStartPodOperator(
        task_id='pandas_Test'
        ,name='pandas-test',
        image='europe-west1-docker.pkg.dev/pod-fr-retail/demok8/demo_k8_jobs_basic:latest',
        cmds= cmds,
        env_vars={'TEST_VARIABLE':'hello3'},
        resources={'request_cpu':3,  'request_memory':'100Mi'},
        get_logs=True,
        startup_timeout_seconds=360,
        is_delete_operator_pod=False
    )