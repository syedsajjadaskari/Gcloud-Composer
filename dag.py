import datetime 
import logging
import os
 
from airflow import configurations
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule

#we set the start_date as previous date
YESTERDAY=datetime.datetime.combine(
    datetime.datetime.today()-datetime.timedelta(1),
    datetime.datetime.min.time()
)
SUCCESS_TAG="sucess"
FAILURE_TAG='failure'

#an airflow variable called gcs_completion_bucket is required.
COMPLETION_BUCKET=models.Variable.get('gcs_completion_bucket')
DS_TAG='{{ ds }}'
DATAFLOW_FILE=os.path.join(
    configurations.get('core','dags_folder'),
    'dataflow','process.py'
)
DEFAULT_DAG_ARGS = {
    'start_date': YESTERDAY,
    'email': models.Variable.get('email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'project_id': models.Variable.get('gcp_project'),
    'dataflow_default_options': {
        'project': models.Variable.get('gcp_project'),
        'temp_location': models.Variable.get('gcp_temp_location'),
        'runner': 'DataflowRunner'
    }
}

def move_to_completion_bucket(target_bucket,target_infix,**kwargs):
    #here we establish connection hook to GogleCloudStorage.
    conn = gcs_hook.GoogleCloudStorage()
    # We extract the bucket and object name from this dictionary.

    source_bucket=kwargs['dag_run'].conf['bucket']
    source_object=kwargs['dag_run'].conf['name']
    completion_ds=kwargs['ds']

    target_object=os.path.join(target_infix,completion_ds,source_object)
    logging.info('Copying %s to %s',
                 os.path.join(source_bucket, source_object),
                 os.path.join(target_bucket, target_object))
    conn.copy(source_bucket, source_object, target_bucket, target_object)

    logging.info('Deleting %s',
                 os.path.join(source_bucket, source_object))
    conn.delete(source_bucket, source_object)


with models.DAG(dag_id='GcsToBigQueryTriggered',
                description='A DAG to triggered by an external cloud functions',
                schedule_interval=None,default_args=DEFAULT_DAG_ARGS) as dag:
    job_args={
        'input': 'gs://{{ dag_run.conf["bucket"] }}/{{ dag_run.conf["name"] }}',
        'output': models.Variable.get('bq_output_table'),
        'fields': models.Variable.get('input_field_names'),
        'load_dt': DS_TAG

    }
    #main dataflow tha will process and load the input delimieted file.
    dataflow_task=dataflow_operator.DataFlowPythonOperator(
        task_id="process=delimited-and push",
        py_file=DATAFLOW_FILE,
        optiosn=job_args
    )
    success_move_task = python_operator.PythonOperator(task_id='success-move-to-completion',
                                                       python_callable=move_to_completion_bucket,
                                                       # A success_tag is used to move
                                                       # the input file to a success
                                                       # prefixed folder.
                                                       op_args=[COMPLETION_BUCKET, SUCCESS_TAG],
                                                       provide_context=True,
                                                       trigger_rule=TriggerRule.ALL_SUCCESS)

    failure_move_task = python_operator.PythonOperator(task_id='failure-move-to-completion',
                                                       python_callable=move_to_completion_bucket,
                                                       # A failure_tag is used to move
                                                       # the input file to a failure
                                                       # prefixed folder.
                                                       op_args=[COMPLETION_BUCKET, FAILURE_TAG],
                                                       provide_context=True,
                                                       trigger_rule=TriggerRule.ALL_FAILED)

    # The success_move_task and failure_move_task are both downstream from the
    # dataflow_task.
    dataflow_task >> success_move_task
    dataflow_task >> failure_move_task

