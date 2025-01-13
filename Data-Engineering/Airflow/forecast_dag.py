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
    "start_date": datetime(2024, 10, 30, tzinfo=local_tz),
    "email": ["ml-ngaedea@singtel.com", "ml-ngvsdondc@singtel.com"],
    "retries": 10,
    "retry_exponential_backoff": True,
    "retry_delay": timedelta(minutes=2),
    "max_retry_delay": timedelta(minutes=5),
    "trigger_rule": "all_success",
}

custom_args = {
    "email_title": "RAN ANOMALY FORECAST",
    "custom_log_path": "/data/airflow/email_logs/nbi/ran_anomaly/forecast/",
    "trigger_email_on_retry_count": [3, 6, 9],
    "external_sensor_dag_id": "nbi-ran_anomaly_transform",
    "external_sensor_task_id": "transform_merge",
    "queue": "mlclus",
     # Adding the EDF airflow URL and authentication.
    # Currently in the EDF url the authentication method is set to default, which allows all users to authenticate
    # The basic auth credentials are set here for the future in case when we set the authentication to basic
    "edf_airflow_url":"https://cck03-ndc-edf02.ndc.singtel.net:8780/",
    "edf_airflow_basic_auth":("edf_admin1","1qazXSW@3edc"),
    "image_name": "cck03-ndc-ua08.ndc.singtel.net/ezua/run_anamoly_image:1.0.0"
}

dag = DAG(
    dag_id="nbi-ran_anomaly_forecast",
    default_args=default_args,
    description="RAN Anomaly: Forecast",
    schedule_interval="15 0 * * *",
    tags=[
        "NBI",
        "SLMB",
        "UPM",
        "RAN",
        "Anomaly Detection",
        "LTE",
        "AI/ML",
        "Forecast",
        "Prediciton",
    ],
    max_active_runs=1,
    catchup=True,
    access_control={'All': {'can_read', 'can_edit'}}
)

############################
##### Global Variables #####
############################
forecast_kpis1 = ["max_avg_ul_rssi_dbm"]
forecast_kpis = [
    "max_avg_ul_rssi_dbm",
    "total_rrc_setup_attempts",
    "total_rrc_setup_failure",
    "total_erab_request",
    "total_erab_drop",
    "total_data_usage_bytes",
    "max_rrc_conn",
    "imsi_cnt",
    # "max_prb_dl",
    # "avg_fdd_ms",
]

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
    if task_instance.task_id == "overall":
        email_title = f"{custom_args['email_title']}: overall"
        log_file = (
            f"{custom_args['custom_log_path']}overall_{dag_processing_datetime}.log"
        )
    else:
        for kpi in forecast_kpis:
            if task_instance.task_id == kpi:
                email_title = f"{custom_args['email_title']}: {kpi}"
                log_file = f"{custom_args['custom_log_path']}{kpi}_{dag_processing_datetime}.log"
    trigger_email = custom_args["trigger_email_on_retry_count"]
    if (task_instance.try_number - 1) in trigger_email:
        send_custom_email(
            task_instance, email_address, email_title, dag_processing_datetime, log_file
        )


#############################
##### Application Start #####
#############################

dag_execution_datetime = "{{ (execution_date.in_timezone('Asia/Singapore') + macros.timedelta(days=1)).strftime('%Y%m%d%H%M') }}"

start = PythonOperator(
    task_id="start",
    python_callable=get_processing_datetime,
    op_kwargs={"execution_datetime": dag_execution_datetime},
    queue=custom_args["queue"],
    dag=dag,
)

dag_processing_datetime = '{{ task_instance.xcom_pull(task_ids="start") }}'
ext_sensor_list = []
for i, j in enumerate(range(1, 49)):  # 48 time-steps (30 minutes granularity)
    ext_sensor = ExternalTaskSensor(
        task_id=f"ext_sensor_{i}",
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
        execution_delta=timedelta(days=-1, minutes=j * 30),
        queue=custom_args["queue"],
        dag=dag,
    )
    ext_sensor_list.append(ext_sensor)

dummy = DummyOperator(task_id="dummy", queue=custom_args["queue"], dag=dag)

train_kpis = []
for kpi in forecast_kpis:

    train_operator= get_k8_operator(
                                    task_id=kpi,
                                    cmd=["sudo","-iu","nbi_etl","bash","-ex","/opt/airflow/airflow/dags/nbi/ran_anamoly/bin/forecast.sh"
                                    ,"{{ task_instance.xcom_pull(task_ids='start',key='return_value') }}",f'{custom_args["custom_log_path"]}',f"{kpi}"],
                                    #cmd=["sleep","infinity"],
                                    container_name=f'run_anamoly_{kpi}',
                                    image=custom_args['image_name'],
                                    min_cpu='2',
                                    max_cpu='4',
                                    min_memory='8000Mi',
                                    max_memory='16000Mi',
                                    gpu='1'
                                   )
    #train_operator = BashOperator(
     #   task_id=kpi,
      #  bash_command=f'sudo -u nbi_etl bash -ex /opt/airflow/airflow/dags/nbi/ran_anomaly/bin/forecast.sh {dag_processing_datetime} {custom_args["custom_log_path"]} {kpi}',
       # on_retry_callback=send_alert,
        #on_failure_callback=send_alert,
        #queue=custom_args["queue"],
        #dag=dag,
    #)
    train_kpis.append(train_operator)


merge_operator= get_k8_operator(
                                    task_id="overall",
                                    cmd=["sudo","-iu","nbi_etl","bash","-ex","/opt/airflow/airflow/dags/nbi/ran_anamoly/bin/forecast.sh"
                                    ,"{{ task_instance.xcom_pull(task_ids='start',key='return_value') }}",f'{custom_args["custom_log_path"]}',"overall"],
                                    container_name=f'run_anamoly_{kpi}',
                                    image=custom_args['image_name'],
                                    min_cpu='2',
                                    max_cpu='4',
                                    min_memory='8000Mi',
                                    max_memory='16000Mi'
                                   )
#merge_operator = BashOperator(
 #   task_id="overall",
  #  bash_command=f'sudo -u nbi_etl bash -ex /opt/airflow/airflow/dags/nbi/ran_anomaly/bin/forecast.sh {dag_processing_datetime} {custom_args["custom_log_path"]} overall',
   # on_retry_callback=send_alert,
    #on_failure_callback=send_alert,
    #queue=custom_args["queue"],
    #dag=dag,
#)

finish = DummyOperator(task_id="finish", queue=custom_args["queue"], dag=dag)

start >> ext_sensor_list >> dummy >> train_kpis >> merge_operator >> finish
#start >> train_kpis >> merge_operator >> finish

