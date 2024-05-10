from airflow import DAG
from airflow.providers.smtp.operators.smtp import SmtpOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 8),
    'email': ['raviteja.panugundla@gmail.com'],  # Your Airflow email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'send_email',
    default_args=default_args,
    description='A secure DAG to send an email using SMTP',
    schedule_interval=timedelta(days=1),
)

email_subject = "Test Email from Airflow"
email_body = "This is a test email sent from Apache Airflow using SMTP!"

send_email_task = SmtpOperator(
    task_id='send_email_task',
    to='tarunratan6@gmail.com',  # Change this to your recipient's email address
    subject=email_subject,
    html_content=email_body,
    dag=dag,
    smtp_conn_id='smtp_gmail',  # Use the configured connection
    # Try-except block for successful connection verification
    try:
        smtp_conn = send_email_task.get_hook()
        smtp_conn.connect()
        logging.info("Successfully connected to SMTP server!")
    except Exception as e:
        logging.error(f"Connection error: {e}")
        raise e  # Re-raise the exception for task failure handling
)

# Additional tasks in your DAG (if any)
