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
    dag_id='k8s_pod_operator_hello_world',
    default_args=default_args,
    description='DAG to run a pod in Kubernetes to execute hello world commands',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    access_control={'All': {'can_read', 'can_edit'}}
) as dag:

    # Define the pod
    pod = k8s.V1Pod()
    
    # Define the emptyDir volume
    empty_dir_volume = k8s.V1Volume(
        name="dags-data",
        empty_dir=k8s.V1EmptyDirVolumeSource()  # Use emptyDir for temporary storage
    )
    pvc_volume = k8s.V1Volume(
        name="conda-env-pvc2",  # This should match the PVC name
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
            claim_name="conda-env-pvc2",  # PVC name
            read_only=False  # Set to True if you want read-only access
        )
    )
    containers = [
        k8s.V1Container(
            name="EDFClient",
            image="cck03-ndc-ua08.ndc.singtel.net/ezua/run_anamoly_image:1.0.0",
            image_pull_policy="Always",
            security_context={
                'privileged': True,
                'runAsUser': 0
            },
            command=["sh", "-c"],
            args=[
                "cat /etc/krb5.conf && "
                "kinit -kt /home/nbi_etl/nbi_etl.keytab nbi_etl && "
                "sleep infinity"
            ],
            volume_mounts=[
                k8s.V1VolumeMount(mount_path="/git", name="dags-data"),  # Mounting the emptyDir volume
                k8s.V1VolumeMount(mount_path="/pvc-data", name="conda-env-pvc2")  # Mounting the PVC

            ],
        ),
        
        k8s.V1Container(
            name="git-sync",
            image="cck03-ndc-ua08.ndc.singtel.net/ezua/gcr.io/mapr-252711/registry.k8s.io/git-sync/git-sync:v4.2.3",
            env=[
                k8s.V1EnvVar(name="GITSYNC_REPO", value="http://cck03-ndc-ua08:7080/root/singtelmlprojects.git"),
                k8s.V1EnvVar(name="GITSYNC_LINK", value="gitdags"),
                k8s.V1EnvVar(name="GITSYNC_REF", value="main"),
                k8s.V1EnvVar(name="GITSYNC_ONE_TIME", value="false"),
                k8s.V1EnvVar(name="GITSYNC_MAX_FAILURES", value="11"),
                k8s.V1EnvVar(name="GITSYNC_PERIOD", value="5s"),
                k8s.V1EnvVar(name="GITSYNC_DEPTH", value="1"),
                k8s.V1EnvVar(name="GITSYNC_SYNC_TIMEOUT", value="600s"),
                k8s.V1EnvVar(name="GITSYNC_PASSWORD", value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(key="password", name="airflow-gitrepo-password"))),
                k8s.V1EnvVar(name="GITSYNC_USERNAME", value="root"),
                k8s.V1EnvVar(name="GITSYNC_GIT_CONFIG", value="safe.directory:*,http.sslVerify:false"),
                k8s.V1EnvVar(name="HTTP_PROXY"),
                k8s.V1EnvVar(name="HTTPS_PROXY"),
            ],
            volume_mounts=[
                k8s.V1VolumeMount(mount_path="/git", name="dags-data")  # Mounting the emptyDir volume
            ],
            ports=[k8s.V1ContainerPort(container_port=2020, name="gitsync", protocol="TCP")],
            resources=k8s.V1ResourceRequirements(
                limits={"cpu": "290m", "memory": "770Mi"},
                requests={"cpu": "80m", "memory": "410Mi"}
            ),
            termination_message_path="/dev/termination-log",
            termination_message_policy="File",
        )
    ]

    host_aliases = [
        k8s.V1HostAlias(
            ip="192.168.120.51",
            hostnames=["cck03-ndc-edf01"]
        ),
        k8s.V1HostAlias(
            ip="192.168.120.55",
            hostnames=["cck03-ndc-edf05"]
        ),
        k8s.V1HostAlias(
            ip="192.168.120.60",
            hostnames=["cck03-ndc-edf10"]
        ),
        k8s.V1HostAlias(
            ip="192.168.120.101",
            hostnames=["cck03-ndc-ad01.ndc.singtel.net"]
        ),
        k8s.V1HostAlias(
            ip="192.168.120.102",
            hostnames=["cck03-ndc-ad02.ndc.singtel.net"]
        ),
    ]

    # Add the emptyDir volume to the pod spec
    pod.spec = k8s.V1PodSpec(
        containers=containers,
        host_aliases=host_aliases,
        host_network=True,
        volumes=[empty_dir_volume,pvc_volume]  # Include the emptyDir volume here
    )

    # Define the KubernetesPodOperator
    k = KubernetesPodOperator(
        task_id="dry_run_demo",
        is_delete_operator_pod=False,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "700Mi"},
            limits={"cpu": "1", "memory": "2000Mi"},
        ),
        full_pod_spec=pod
    )