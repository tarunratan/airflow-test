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
    "start_date": datetime(2024, 10, 25, tzinfo=local_tz),
    "email": ["ml-ngaedea@singtel.com", "ml-ngvsdondc@singtel.com"],
    "retries": 20,
    "retry_exponential_backoff": False,
    "retry_delay": timedelta(minutes=1),
    "max_retry_delay": timedelta(minutes=3),
    "trigger_rule": "all_success",
}

# need to change the custom log pathlater
custom_args = {
    "email_title": "RAN ANOMALY TRANSFORM",
    "custom_log_path": "/data/airflow/email_logs/nbi/ran_anomaly/",
    "trigger_email_on_retry_count": [
        3,
        10,
        16,
        21,
    ],
    "external_sensor_slmb_app_dag_id": "slmb_app",
    "external_sensor_slmb_app_task_id": "ingress_slmb_app",
    "external_sensor_upm_dag_id": "upm_ingress",
    "external_sensor_upm_aiml_task_id": "ingress_upm_ai_ml_performance",
    "external_sensor_upm_lte_task_id": "ingress_upm_lte",
    "upm_execution_delta_1": timedelta(minutes=-17),
    "upm_execution_delta_2": timedelta(minutes=-32),  # + 15 minutes,
    # Adding the EDF airflow URL and authentication.
    # Currently in the EDF url the authentication method is set to default, which allows all users to authenticate
    # The basic auth credentials are set here for the future in case when we set the authentication to basic
    "edf_airflow_url":"https://cck03-ndc-edf02.ndc.singtel.net:8780/",
    "edf_airflow_basic_auth":("edf_admin1","1qazXSW@3edc"),
    "image_name": "cck03-ndc-ua08.ndc.singtel.net/ezua/run_anamoly_image:1.0.0"
}

dag = DAG(
    dag_id="nbi-ran_anomaly_transform",
    default_args=default_args,
    description="RAN Anomaly: Transform",
    schedule_interval="15,45 * * * *",
    tags=["NBI", "SLMB", "UPM", "RAN", "Anomaly Detection", "LTE"],
    max_active_runs=10,
    catchup=True,
    access_control={'All': {'can_read', 'can_edit'}}
)

############################
##### Global Variables #####
############################

transform_steps = ["transform_slmb", "transform_upm", "transform_merge"]

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
    for step in transform_steps:
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
    queue="ml",
    dag=dag,
)

dag_processing_datetime = '{{ task_instance.xcom_pull(task_ids="start") }}'


# "external_sensor_slmb_app_dag_id": "slmb_app",
# "external_sensor_slmb_app_task_id": "ingress_slmb_app",
# "external_sensor_upm_dag_id": "upm_ingress",
# "external_sensor_upm_aiml_task_id": "ingress_upm_ai_ml_performance",
# "external_sensor_upm_lte_task_id": "ingress_upm_lte",

concurrent_sensors = []
upm_sensors = []
ingress_sensor_slmb_app = ExternalAirflowTaskSensor(
    external_airflow_url=custom_args['edf_airflow_url'],
    api_auth=custom_args['edf_airflow_basic_auth'],
    task_id=f"sensor_{custom_args['external_sensor_slmb_app_task_id']}",
    external_dag_id=custom_args["external_sensor_slmb_app_dag_id"],
    external_task_id=custom_args["external_sensor_slmb_app_task_id"],
    allowed_states=["success"],
    failed_states=[
        "failed",
        "skipped",
        "upstream_failed",
    ],
    mode="reschedule",
    poke_interval=60,
    queue="ml",
    dag=dag,
)
concurrent_sensors.append(ingress_sensor_slmb_app)

for i in range(2):
    ingress_sensor_upm_aiml = ExternalAirflowTaskSensor(
        external_airflow_url=custom_args['edf_airflow_url'],
        api_auth=custom_args['edf_airflow_basic_auth'],
        task_id=f"sensor_{custom_args['external_sensor_upm_aiml_task_id']}_{i+1}",
        external_dag_id=custom_args["external_sensor_upm_dag_id"],
        external_task_id=custom_args["external_sensor_upm_aiml_task_id"],
        allowed_states=["success"],
        failed_states=[
            "failed",
            "skipped",
            "upstream_failed",
        ],
        mode="reschedule",
        poke_interval=60,
        execution_delta=custom_args[f"upm_execution_delta_{i+1}"],
        queue="ml",
        dag=dag,
    )
    ingress_sensor_upm_lte = ExternalAirflowTaskSensor(
        external_airflow_url=custom_args['edf_airflow_url'],
        api_auth=custom_args['edf_airflow_basic_auth'],
        task_id=f"sensor_{custom_args['external_sensor_upm_lte_task_id']}_{i+1}",
        external_dag_id=custom_args["external_sensor_upm_dag_id"],
        external_task_id=custom_args["external_sensor_upm_lte_task_id"],
        allowed_states=["success"],
        failed_states=[
            "failed",
            "skipped",
            "upstream_failed",
        ],
        mode="reschedule",
        poke_interval=60,
        execution_delta=custom_args[f"upm_execution_delta_{i+1}"],
        queue="ml",
        dag=dag,
    )
    concurrent_sensors.append(ingress_sensor_upm_aiml)
    concurrent_sensors.append(ingress_sensor_upm_lte)
    upm_sensors.append(ingress_sensor_upm_aiml)
    upm_sensors.append(ingress_sensor_upm_lte)

transform_operators = []
git_dags_location="/git/gitdags"
for step in transform_steps:
    #transform_operator = BashOperator(
     #   task_id=step,
      #  bash_command=f'sudo -u nbi_etl bash -ex /opt/airflow/airflow/dags/nbi/ran_anomaly/bin/transform.sh {dag_processing_datetime} {custom_args["custom_log_path"]} {step}',
       # on_retry_callback=send_alert,
       ## on_failure_callback=send_alert,
        #execution_timeout=timedelta(minutes=10),
        #queue="ml",
        #dag=dag,
    #)
    transform_operator = get_k8_operator(
                                        task_id=step,
                                        cmd=["sudo","-iu","nbi_etl","bash","-ex","/opt/airflow/airflow/dags/nbi/ran_anamoly/bin/transform.sh"
                                        ,"{{ task_instance.xcom_pull(task_ids='start',key='return_value') }}",f'{custom_args["custom_log_path"]}',f"{step}"],
                                        #cmd=["sleep","infinity"]
                                        container_name=f'run_anamoly_{step}',
                                        image=custom_args['image_name']
                                        )
                                        
    transform_operators.append(transform_operator)

finish = DummyOperator(task_id="finish", queue="ml", dag=dag)
start >> concurrent_sensors
ingress_sensor_slmb_app >> transform_operators[0]
upm_sensors >> transform_operators[1]
transform_operators[:-1] >> transform_operators[-1] >> finish

