from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='k8stest2',
    default_args=default_args,
    description='DAG to run a pod in Kubernetes to execute hello world commands',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    access_control={'All': {'can_read', 'can_edit'}}
) as dag:

    # Define the KubernetesPodOperator
    run_hello_world_in_k8s = KubernetesPodOperator(
        namespace='dev1',  # Specify the namespace for your Kubernetes cluster
        image='alpine:latest',  # Open-source Alpine Linux image
        cmds=["sh", "-c"],
        arguments=["echo 'Hello, World!' && echo 'This is a test command'"],  # Commands to run in the pod
        labels={"app": "hello-world-test"},
        name="opensource-custom-image",
        task_id="run_opensource_world_in_k8s",
        is_delete_operator_pod=False,  # Clean up the pod after it finishes running
        container_resources=k8s.V1ResourceRequirements(  # Updated to use container_resources
            limits={"cpu": "200m", "memory": "256Mi"},  # Resource limits
            requests={"cpu": "100m", "memory": "128Mi"}  # Resource requests
        ),
    )

    run_opensource_world_in_k8s
