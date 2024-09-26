from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
import mlflow
import logging


def search_mlflow_runs():
mlflow.set_tracking_uri("http://mlflow.mlflow.svc.cluster.local:5000")

experiment_name = "test_tszamel"
experiment = mlflow.get_experiment_by_name(experiment_name)

if experiment:
runs = mlflow.search_runs(filter_string="tags.mlflow.runName = 'prophet'")
logging.info(runs)
else:
raise ValueError(f"Experiment {experiment_name} not found.")


default_args = {
"owner": "airflow",
"depends_on_past": False,
"start_date": datetime(2024, 9, 19, 9, 30),
"email_on_failure": False,
"email_on_retry": False,
}

with DAG(
"mlflow_airflow_connection_test",
default_args=default_args,
description="DAG to search MLflow experiment runs",
schedule_interval=None,
tags=["mlflow"],
catchup=False,
access_control={
"Admin": {"can_edit", "can_read", "can_delete"},
},
) as dag:
search_runs_task = PythonOperator(
task_id="search_mlflow_runs",
python_callable=search_mlflow_runs,
)

search_runs_task
