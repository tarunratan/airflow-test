from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Import the custom function from the other DAG
from custom_function_dag import my_custom_function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'import_function_dag',
    default_args=default_args,
    description='A DAG that imports a custom function from another DAG',
    schedule_interval=None,
)

def use_imported_function(**kwargs):
    result = my_custom_function()
    print(f"Result from imported function: {result}")
    return result

import_task = PythonOperator(
    task_id='use_imported_function_task',
    python_callable=use_imported_function,
    provide_context=True,
    dag=dag,
)
