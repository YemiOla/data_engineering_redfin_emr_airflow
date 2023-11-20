from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
import boto3
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, 
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor




job_flow_overrides = {
    "Name": "redfin_emr_cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
    "LogUri": "s3://redfin-data-project-yml/emr-logs-yml/",
    "VisibleToAllUsers":False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
         
        "Ec2SubnetId": "subnet-0e3390f70289d5006",
        "Ec2KeyName" : 'emr-keypair-airflow',
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # Setting this as false will allow us to programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
   
}

SPARK_STEPS_EXTRACTION = [
    {
        "Name": "Extract Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": [
                "s3://redfin-data-project-yml/scripts/ingest.sh",
            ],
        },
    },
   ]


SPARK_STEPS_TRANSFORMATION = [
    {
        "Name": "Transform Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
            "s3://redfin-data-project-yml/scripts/transform_redfin_data.py",
            ],
        },
    },
   ]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 17), 
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

with DAG('redfin_analytics_spark_job_dag',
        default_args=default_args,
        # schedule_interval = '@weekly',
        catchup=False) as dag:

        start_pipeline = DummyOperator(task_id="tsk_start_pipeline")

         # Create an EMR cluster
        create_emr_cluster = EmrCreateJobFlowOperator(
            task_id="tsk_create_emr_cluster",
            job_flow_overrides=job_flow_overrides,
            # aws_conn_id="aws_default",
            # emr_conn_id="emr_default",
        )

        is_emr_cluster_created = EmrJobFlowSensor(
        task_id="tsk_is_emr_cluster_created", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        target_states={"WAITING"},  # Specify the desired state
        timeout=3600,
        poke_interval=5,
        mode='poke',
        )

        # Add your steps to the EMR cluster
        add_extraction_step = EmrAddStepsOperator(
        task_id="tsk_add_extraction_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        # aws_conn_id="aws_default",
        steps=SPARK_STEPS_EXTRACTION,
        # do_xcom_push=True, # Enable XCom push to monitor step status
        )

        is_extraction_completed = EmrStepSensor(
        task_id="tsk_is_extraction_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='tsk_add_extraction_step')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=5,
        )

        add_transformation_step = EmrAddStepsOperator(
        task_id="tsk_add_transformation_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        # aws_conn_id="aws_default",
        steps=SPARK_STEPS_TRANSFORMATION,
        # do_xcom_push=True, # Enable XCom push to monitor step status
        )

        is_transformation_completed = EmrStepSensor(
        task_id="tsk_is_transformation_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='tsk_add_transformation_step')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=10,
        )

        remove_cluster = EmrTerminateJobFlowOperator(
        task_id="tsk_remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        )

        is_emr_cluster_terminated = EmrJobFlowSensor(
        task_id="tsk_is_emr_cluster_terminated", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        target_states={"TERMINATED"},  # Specify the desired state
        timeout=3600,
        poke_interval=5,
        mode='poke',
        )

        end_pipeline = DummyOperator(task_id="tsk_end_pipeline")


        start_pipeline >> create_emr_cluster >> is_emr_cluster_created >> add_extraction_step >> is_extraction_completed
        is_extraction_completed >> add_transformation_step >> is_transformation_completed >> remove_cluster
        remove_cluster >> is_emr_cluster_terminated >> end_pipeline




