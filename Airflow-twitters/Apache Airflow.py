from datetime import timedelta
import datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
default_args={
    'owner':'airflow',
    'start_date':airflow.utils.dates.days_ago(2),
    "end_date":datetime(2021,6,23),
    "depends_on_past":False,
    'email':['airflow@example.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}
dag=DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(day=1),
)
t1=BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

