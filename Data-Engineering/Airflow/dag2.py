from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import my_custom_function

with DAG(
    dag_id='my_dag_2',
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:

    def another_task(**kwargs):
        result = my_custom_function(20, 3)
        print(f"Result: {result}")

    task2 = PythonOperator(
        task_id='another_task',
        python_callable=another_task
    )
