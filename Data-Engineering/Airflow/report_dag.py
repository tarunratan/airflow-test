import os
import sys

import pendulum

dags_folder = os.getenv('AIRFLOW__CORE__DAGS_FOLDER')
# Insert the absolute path of the DAGs folder or its 'library' subdirectory
sys.path.insert(0, os.path.abspath(os.path.join(dags_folder, "library")))

from datetime import datetime, timedelta

from airflow import DAG, macros
from airflow.models import XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow_utils import *

#################################
##### Airflow Configuration #####
#################################

local_tz = pendulum.timezone("Asia/Singapore")

default_args = {
    "owner": "nbi_etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2024, 10, 31, tzinfo=local_tz),
    "email": ["ml-ngaedea@singtel.com", "ml-ngvsdondc@singtel.com"],
    "retries": 3,
    "retry_exponential_backoff": True,
    "retry_delay": timedelta(minutes=2),
    "max_retry_delay": timedelta(minutes=5),
    "trigger_rule": "all_success",
}

custom_args = {
    "email_title": "RAN ANOMALY REPORT",
    "custom_log_path": "/data/airflow/email_logs/nbi/ran_anomaly/report/",
    "trigger_email_on_retry_count": [1, 2],
    "external_sensor_dag_id": "nbi-ran_anomaly_detection",
    "external_sensor_task_id": "detection",
    "queue": "ml",
     "image_name": "cck03-ndc-ua08.ndc.singtel.net/ezua/run_anamoly_image:1.0.0"
}

dag = DAG(
    dag_id="nbi-ran_anomaly_report",
    default_args=default_args,
    description="RAN Anomaly: Daily Report",
    schedule_interval="0 10 * * *",
    tags=["NBI", "RAN", "Anomaly Detection", "Report"],
    max_active_runs=1,
    catchup=True,
    access_control={'All': {'can_read', 'can_edit'}}
)

############################
##### Global Variables #####
############################

# steps = ["inbuilding_kpi_abnormal", "outdoor_kpi_abnormal"]

###########################
##### Helper Function #####
###########################


def get_processing_datetime(**context):
    execution_time = context["execution_datetime"]
    year = execution_time[0:4]
    month = execution_time[4:6]
    day = execution_time[6:8]
    hour = execution_time[8:10]
    mins = int(execution_time[10:12])
    if mins < 30:
        mins = "00"
    else:
        mins = "30"
    processing_datetime = year + month + day + hour + mins
    return processing_datetime


def send_alert(context):
    task_instance = context["task_instance"]  # contains context details
    dag_processing_datetime = XCom.get_one(
        execution_date=context.get("execution_date"), task_id="start"
    )
    email_address = default_args["email"]
    email_title = f"{custom_args['email_title']}"
    log_file = f"{custom_args['custom_log_path']}report_{dag_processing_datetime}.log"
    trigger_email = custom_args["trigger_email_on_retry_count"]
    if (task_instance.try_number - 1) in trigger_email:
        send_custom_email(
            task_instance, email_address, email_title, dag_processing_datetime, log_file
        )


#############################
##### Application Start #####
#############################

dag_execution_datetime = (
    "{{ (execution_date.in_timezone('Asia/Singapore')).strftime('%Y%m%d%H%M') }}"
)

start = PythonOperator(
    task_id="start",
    python_callable=get_processing_datetime,
    op_kwargs={"execution_datetime": dag_execution_datetime},
    queue=custom_args["queue"],
    dag=dag,
)

dag_processing_datetime = '{{ task_instance.xcom_pull(task_ids="start") }}'

ext_sensor = ExternalTaskSensor(
    task_id=f"ext_sensor",
    external_dag_id=custom_args["external_sensor_dag_id"],
    external_task_id=custom_args["external_sensor_task_id"],
    allowed_states=["success"],
    failed_states=[
        "failed",
        "skipped",
        "upstream_failed",
    ],
    mode="reschedule",
    poke_interval=60,
    execution_delta=timedelta(hours=-13, minutes=-45),
    queue=custom_args["queue"],
    dag=dag,
)
#report_operator = BashOperator(
#    task_id="kpi_abnormal_report",
#    bash_command=f'sudo -u nbi_etl bash -ex /opt/airflow/airflow/dags/nbi/ran_anomaly/bin/report.sh {dag_processing_datetime} {custom_args["custom_log_path"]}',
#    on_retry_callback=send_alert,
#    on_failure_callback=send_alert,
#    queue=custom_args["queue"],
#    dag=dag,
#)

report_operator= get_k8_operator(
                                    task_id="kpi_abnormal_report",
                                    #cmd=["sleep","infinity"],
                                    cmd=["sudo","-iu","nbi_etl","bash","-ex","/opt/airflow/airflow/dags/nbi/ran_anamoly/bin/report.sh"
                                    ,"{{ task_instance.xcom_pull(task_ids='start',key='return_value') }}",f'{custom_args["custom_log_path"]}'],
                                    container_name=f'run_anamoly_report',
                                    image=custom_args['image_name']
                                   )
finish = DummyOperator(task_id="finish", queue=custom_args["queue"], dag=dag)

start >> ext_sensor >> report_operator >> finish

