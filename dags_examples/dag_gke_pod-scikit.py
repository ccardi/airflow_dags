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
    'start_date': datetime(2018, 7, 12),
    'email': ['cardi@google.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #GKE Operator global config (**gke_conf)
    'project_id' : 'pod-fr-retail'
    ,'namespace':'default'
    ,'location' :'europe-west1'  #'europe-west1-c'
    ,'cluster_name' : 'autopilot-cluster-2'  #'europe-west1-luxury-compose-f60ca3a5-gke' 
    #,'image_pull_policy' :  'Always'
}


# Define the pipeline schedule
schedule_interval = None
with DAG("dag_gke_pod-scikit", default_args=default_args, catchup=False, schedule_interval=schedule_interval) as dag:

    scikit_cmd= [
        'python' ,'/app/src/task.py'
        ,'--training_data_path','gs://cloud-samples-data/ml-engine/iris/classification/train.csv'
        ,'--test_data_path','gs://cloud-samples-data/ml-engine/iris/classification/evaluate.csv'
        ,'--output_dir','gs://pod-fr-retail/kfscikit'
        ,'--hyperparameters','n_estimators 100 max_depth 4'
        ,'--output_dir_path','/outputs/Output_Dir/data'
        ,'--estimator_name','GradientBoostingClassifier'
    ]
    for x in range(0, 100):
        build_model = GKEStartPodOperator(
            task_id='scikit_Test'+str(x),
            name='scikit-test',
            image='gcr.io/kf-pipeline-contrib/sklearn:latest',
            cmds= scikit_cmd,
            resources={'request_cpu':0.05,  'request_memory':'100Mi'},
            retries=3,
            get_logs=True,
            startup_timeout_seconds=360,
            retry_delay=timedelta(minutes=1),
            is_delete_operator_pod=False
        )