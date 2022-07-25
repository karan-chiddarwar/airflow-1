from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['karan.chiddarwar@clairvoyantsoft.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# Spark job execution details
SPARK_STEPS = [
    {
        'Name': 'basic_sparkjob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://kc-playground-s3-training/airflow/script/display_custom_message.py",
                '--exec_date={{ execution_date.strftime("%Y-%m-%d") }}',
            ],
        },
    }
]

# EMR configuration overrides
JOB_FLOW_OVERRIDES = {
    'Name': 'kc-airflow-1-emr-sparkjob-01',
    'ReleaseLabel': 'emr-5.29.0',
    "LogUri": "s3://kc-playground-s3-training/airflow/output/",
    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Sqoop"},
        {"Name": "Hive"},
    ],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    },
                }
            ],
        }
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

# Dag
with DAG(
    dag_id='emr_spark_job_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
) as dag:

    # [Step to initiate an EMR]
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )
    # [Step to ingest a job process]
    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )
    # [Step to monitor the task]
    step_checker = EmrStepSensor(
        task_id='monitor_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )
    # [Step to terminate the EMR cluster after execution is complete]
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='terminate_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
    )
    # [Dag flow initialization]
    cluster_creator >> step_adder >> step_checker >> cluster_remover