from airflow import DAG
from airflow.providers.smtp.operators.smtp import SmtpOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 10),
}

dag = DAG(
    'smtp_test2',
    default_args=default_args,
    description='A simple DAG to test SMTP connection',
    schedule_interval=None,  # Run this DAG manually
)

test_email_body = "This is a test email to verify SMTP connection."

send_test_email = SmtpOperator(
    task_id='send_test_email',
    to='tarunratan6@gmail.com',  # Replace with your email address
    subject="Airflow SMTP Test",
    html_content=test_email_body,
    dag=dag,
    smtp_conn_id='smtp_gmail',  # Use the same connection ID as your original DAG
)

send_test_email
