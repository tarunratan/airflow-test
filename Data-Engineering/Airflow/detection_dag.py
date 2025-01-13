import os
import sys

import pendulum

dags_folder = os.getenv('AIRFLOW__CORE__DAGS_FOLDER')
# Insert the absolute path of the DAGs folder or its 'library' subdirectory
sys.path.insert(0, os.path.abspath(os.path.join(dags_folder, "library")))


from datetime import datetime, timedelta

from airflow import DAG
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
    "retries": 20,
    "retry_exponential_backoff": False,
    "retry_delay": timedelta(minutes=1),
    "max_retry_delay": timedelta(minutes=3),
    "trigger_rule": "all_success",
}

custom_args = {
    "email_title": "RAN ANOMALY DETECTION",
    "custom_log_path": "/data/airflow/email_logs/nbi/ran_anomaly/detection/",
    "trigger_email_on_retry_count": [
        3,
        10,
        16,
        21,
    ],
    "external_sensor_transform_dag_id": "nbi-ran_anomaly_transform",
    "external_sensor_transform_task_id": "transform_merge",
    "external_sensor_forecast_dag_id": "nbi-ran_anomaly_forecast",
    "external_sensor_forecast_task_id": "overall",
    "queue": "mlclus",
     "image_name": "cck03-ndc-ua08.ndc.singtel.net/ezua/run_anamoly_image:1.0.0"
}

dag = DAG(
    dag_id="nbi-ran_anomaly_detection",
    default_args=default_args,
    description="RAN Anomaly: Detection",
    schedule_interval="15,45 * * * *",
    tags=["NBI", "RAN", "Anomaly Detection", "LTE"],
    max_active_runs=1,
    catchup=True,
    access_control={'All': {'can_read', 'can_edit'}}
)

############################
##### Global Variables #####
############################

steps = ["demerit", "detection"]

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
    for step in steps:
        if step in task_instance.task_id:
            email_title = f"{custom_args['email_title']}: {step}"
            log_file = (
                f"{custom_args['custom_log_path']}{step}_{dag_processing_datetime}.log"
            )
    trigger_email = custom_args["trigger_email_on_retry_count"]
    if (task_instance.try_number - 1) in trigger_email:
        send_custom_email(
            task_instance, email_address, email_title, dag_processing_datetime, log_file
        )


#############################
##### Application Start #####
#############################
dag_execution_datetime = (
    "{{ execution_date.in_timezone('Asia/Singapore').strftime('%Y%m%d%H%M') }}"
)

start = PythonOperator(
    task_id="start",
    python_callable=get_processing_datetime,
    op_kwargs={"execution_datetime": dag_execution_datetime},
    queue=custom_args["queue"],
    dag=dag,
)

dag_processing_datetime = '{{ task_instance.xcom_pull(task_ids="start") }}'

concurrent_sensors = []
transform_sensor = ExternalTaskSensor(
    task_id=f"sensor_{custom_args['external_sensor_transform_dag_id']}",
    external_dag_id=custom_args["external_sensor_transform_dag_id"],
    external_task_id=custom_args["external_sensor_transform_task_id"],
    allowed_states=["success"],
    failed_states=[
        "failed",
        "skipped",
        "upstream_failed",
    ],
    mode="reschedule",
    poke_interval=60,
    queue=custom_args["queue"],
    dag=dag,
)
concurrent_sensors.append(transform_sensor)

forecast_sensor = ExternalTaskSensor(
    task_id=f"sensor_{custom_args['external_sensor_forecast_dag_id']}",
    external_dag_id=custom_args["external_sensor_forecast_dag_id"],
    external_task_id=custom_args["external_sensor_forecast_task_id"],
    allowed_states=["success"],
    failed_states=[
        "failed",
        "skipped",
        "upstream_failed",
    ],
    mode="reschedule",
    poke_interval=60,
    execution_date_fn=(
        lambda dt: (dt - timedelta(hours=16)).replace(
            hour=16, minute=15, second=0, microsecond=0
        )
        - timedelta(days=1)
    ),
    queue=custom_args["queue"],
    dag=dag,
)
concurrent_sensors.append(forecast_sensor)

#demerit_operator = BashOperator(
#   task_id=steps[0],
#  bash_command=f'sudo -u nbi_etl bash -ex /opt/airflow/airflow/dags/nbi/ran_anomaly/bin/detection.sh {dag_processing_datetime} {custom_args["custom_log_path"]} #{steps[0]}',
# on_retry_callback=send_alert,
#on_failure_callback=send_alert,
#queue=custom_args["queue"],
#dag=dag,
#)
demerit_operator = get_k8_operator(
    task_id=steps[0],
   # cmd=["sleep","infinity"],
    cmd=[
        "sudo", "-iu", "nbi_etl", "bash", "-ex", "/opt/airflow/airflow/dags/nbi/ran_anamoly/bin/detection.sh",
        "{{ task_instance.xcom_pull(task_ids='start', key='return_value') }}",
        f'{custom_args["custom_log_path"]}',
        f'{steps[0]}'
    ],
    container_name='run_anamoly_demerit',
    image=custom_args['image_name']
)


detection_operator = get_k8_operator(
    task_id=steps[1],
    cmd=[
        "sudo", "-iu", "nbi_etl", "bash", "-ex", "/opt/airflow/airflow/dags/nbi/ran_anamoly/bin/detection.sh",
        "{{ task_instance.xcom_pull(task_ids='start', key='return_value') }}",
        f'{custom_args["custom_log_path"]}',
        f'{steps[1]}'
    ],
    container_name='run_anamoly_detection',
    image=custom_args['image_name']
)

#detection_operator = BashOperator(
#    task_id=steps[1],
#    bash_command=f'sudo -u nbi_etl bash -ex /opt/airflow/airflow/dags/nbi/ran_anomaly/bin/detection.sh {dag_processing_datetime} {custom_args["custom_log_path"]} #{steps[1]}',
#    on_retry_callback=send_alert,
#    on_failure_callback=send_alert,
#    queue=custom_args["queue"],
#    dag=dag,
#)

finish = DummyOperator(task_id="finish", queue=custom_args["queue"], dag=dag)

start >> concurrent_sensors >> demerit_operator >> detection_operator >> finish

