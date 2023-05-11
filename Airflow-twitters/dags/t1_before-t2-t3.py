from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.sendors.http_sensor import HttpSensor
from datetime import timedelta
dag=DAG(
    dag_id="hello-world",
    schedule_interval='@daily',
    start_date=days_ago(1)
)
class Myoperator(BaseOperator):
    @apply_defaults
    def __init__(self,name,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.name=name

    def execute(self,context):
        message='Hello {}'.format(self.name)
        print(message)
        return message
task1=BashOperator(
    task_id='t1',
    bash_command='echo hello',
    dag=dag,
    retries=3
)
task2=BashOperator(
    task_id='t2',
    bash_command='echo hello',
    dag=dag
)

task3=BashOperator(
    task_id='t3',
    bash_command='echo hello',
    dag=dag,
    trigger_rule='all_failed'
)

task4=Myoperator(
    name='Raj kUMAR',
    task_id='t4',
    dag=dag,
    trigger_rule='one_success'
)
sensor=HttpSensor(
    task_id='sensor',
    endpoint='/',
    http_conn_id='my_httpcon',
    dag=dag,
    retries=20,
    retry_delay=timedelta(seconds=10)
)

sensor>>task1 >> [task2,task3]>>task4
