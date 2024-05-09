from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kalai',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'demo_dag_with_trigger',
    default_args=default_args,
    description='A simple demo DAG with TriggerDagRunOperator',
    schedule_interval=timedelta(days=1),
)

# Task 1: Trigger another DAG
trigger_task = TriggerDagRunOperator(
    task_id='trigger_another_dag',
    trigger_dag_id='another_dag_id',  
    dag=dag,
)



