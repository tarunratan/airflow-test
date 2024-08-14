from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import my_custom_function

with DAG(
    dag_id='my_dag_1',
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:

    def my_task(**kwargs):
        result = my_custom_function(10, 5)
        print(f"Result: {result}")

    task1 = PythonOperator(
        task_id='my_task',
        python_callable=my_task
    )
