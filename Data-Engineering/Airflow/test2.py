from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'send_email_example',
    default_args=default_args,
    description='A simple DAG to send a test email',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

send_email_task = EmailOperator(
    task_id='send_email_task',
    to='tarunrata6@gmail.com',  # Replace with your recipient email address
    subject='Test Email from Airflow',
    html_content='<p>This is a test email sent from Airflow DAG using Gmail SMTP.</p>',
    dag=dag,
    mime_charset='utf-8',
)
# Set up SMTP connection ID
send_email_task.smtp_conn_id = 'smtp_gmail'
send_email_task
