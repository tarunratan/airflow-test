from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def my_custom_function():
    return "Hello from the custom function!"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'custom_function_dag',
    default_args=default_args,
    description='A DAG with a custom function',
    schedule_interval=None,
)

def test_function(**kwargs):
    result = my_custom_function()
    print(result)
    return result

test_task = PythonOperator(
    task_id='test_function_task',
    python_callable=test_function,
    provide_context=True,
    dag=dag,
)
