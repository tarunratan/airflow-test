from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(dag_id="covid_dag", start_date=datetime(2024, 4, 25)) as dag:

    # Task to download data
    download_data = BashOperator(task_id="download_data", bash_command="wget https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv -O data.csv")

    # Task to process data (replace with your processing logic)
    process_data = BashOperator(task_id="process_data", bash_command="cut -d ',' -f 1,4 data.csv > processed_data.csv")

    # Define the execution order
    download_data >> process_data
