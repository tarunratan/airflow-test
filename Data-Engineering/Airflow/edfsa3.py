import os
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Define the default arguments
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to identify the log file path
def get_log_file_path(**context):
    # Get the log directory from Airflow config
    from airflow.configuration import conf
    log_base = conf.get('logging', 'BASE_LOG_FOLDER')
    
    # Identify the log file path for this DAG's task
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['ts']
    log_file = os.path.join(log_base, dag_id, task_id, execution_date.replace(':', '_'))
    context['ti'].xcom_push(key='log_file_path', value=log_file)
    print(f"Log file path identified: {log_file}")

# Define the DAG
with DAG(
    'send_own_logs_to_s3',
    default_args=default_args,
    description='A DAG to upload its own logs to EDF S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['s3', 'logs'],
) as dag:

    # Task to find the log file path
    identify_log_file = PythonOperator(
        task_id='identify_log_file',
        python_callable=get_log_file_path,
        provide_context=True,
    )

    # Task to upload the identified log file to S3
    upload_logs_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_logs',
        filename="{{ ti.xcom_pull(task_ids='identify_log_file', key='log_file_path') }}",
        dest_key='dag_logs/{{ ds }}/log.txt',  # S3 key; customize as needed
        dest_bucket_name='prestodemo',  # Replace with your bucket name
        aws_conn_id='edfs3',  # Airflow connection ID for S3
        replace=True,
    )

    identify_log_file >> upload_logs_to_s3
