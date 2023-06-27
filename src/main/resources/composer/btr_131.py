# [START composer_dataflow_btr-producer-avro]


"""Example Airflow DAG that creates a Cloud Dataflow workflow for:
 - BTRAccountAvro

 Doc:
 - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataflow/index.html
"""

import random
import string

from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
# from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/tests/system/providers/google/cloud/dataflow/example_dataflow_template.html
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago

bucket_path = Variable.get("GCS_MYPROJECT_ES_DATAFLOW_STAGING")
templates_path = Variable.get("GCS_MYPROJECT_ES_DATAFLOW_TEMPLATES")
project_id = Variable.get("PROJECT_ID")
gce_zone = Variable.get("REGION")
subnetwork = Variable.get("DATAFLOW_SUBNET")
serviceAccount = Variable.get("SA_DATAFLOW")
app_name = "btr-producer-avro"
UNIQUE_ID = (''.join(random.choice(string.ascii_lowercase) for _ in range(5)))
# time_now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

def main():
    dag_args = {
        'dag_id': f'ebm_avro-producer_dag_131',
        'catchup': False,
        'schedule_interval': '@once',
        "default_args": {
            'start_date': days_ago(1),
            'retries': 0,
            'depends_on_past': False,
            'email': ['alberto.lopez@db.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'dataflow_default_options': {
                'project': project_id
            },
            'provide_context': True
        }
    }
    create_dag(dag_args)


def create_dag(dag_args):
    with models.DAG(**dag_args) as dag:
        parameters = {
            #"pubsub-sub": f"projects/{project_id}/subscriptions/pc_kw111t-exp",
            "big-board-type": "bigquery",
            "join-strategy": "bbside",
            # "bb-window": "default"
            "bb-window": "unique",
            "streaming": "true"
            #"big-board-type": "bigquery"# WIP: castException using DataFlow runner
        }
        print(f"Setting up args for avro-producer DataFlow ={parameters}")

        environment = {
            'tempLocation': f"gs://{bucket_path}/temp/",
            'subnetwork': subnetwork,
            'ipConfiguration': 'WORKER_IP_PRIVATE',
            'serviceAccountEmail': serviceAccount,
            'additionalExperiments': 'upload_graph;enable_secure_boot;use_network_tags_for_flex_templates=baseline-int-dev-97434-1-artifactory',
            # 'maxNumWorkers': 3,
            # 'machineType': 'n1-standard-2'
            'machineType': 'e2-highmem-4'
        }
        print(f"Setting up environment={environment}")

        btr_launch = PythonOperator(
            task_id='btr_launch_python_operator_id',
            python_callable=btr_dag,
            op_kwargs={
                'params': parameters,
                'env': environment
            },
            provide_context=True
        )
        btr_launch
    globals()[dag.dag_id] = dag


def btr_dag(params, env, **kwargs):
    dataflow_job_name = '-'.join([app_name, UNIQUE_ID])
    template_name = f"gs://{templates_path}/btr/pe-avro-producer-producer-template.json"
    print('env: ' + str(env))
    print('unique_id: ' + UNIQUE_ID)
    print('dataflow_job_name: ' + dataflow_job_name)
    print(f"template={template_name}")
    print(f"project_id={project_id}")
    print(f"location={gce_zone}")
    btr_flex_template = DataflowStartFlexTemplateOperator(
        task_id=''.join([dataflow_job_name, "-df-task"]),
        project_id=project_id,
        location=gce_zone,
        body={
            "launchParameter": {
                "containerSpecGcsPath": template_name,
                "jobName": dataflow_job_name,
                "environment": env,
                "parameters": params
            }
        },
        do_xcom_push=True,
        wait_until_finished=True
    )

    btr_flex_template.execute(context=kwargs)

main()
