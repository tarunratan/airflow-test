from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def failing_task():
    """This function will raise an exception and fail."""
    raise Exception("This is an intentional failure!")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='dag_with_intentional_failure',
    default_args=default_args,
    description='A simple DAG that fails intentionally',
    schedule_interval=None,  # Run manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Define a task that will fail
    failing_task_operator = PythonOperator(
        task_id='failing_task',
        python_callable=failing_task,
    )

    # Add the task to the DAG
    failing_task_operator
