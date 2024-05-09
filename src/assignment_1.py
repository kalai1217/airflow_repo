from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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
    'demo_dag',
    default_args=default_args,
    description='A simple demo DAG',
    schedule_interval=timedelta(days=1),
)

# Task 1: Dummy task
task_1 = DummyOperator(
    task_id='task_1',
    dag=dag,
)

# Task 2: Python task
def python_task_2():
    print("Executing python task 2")

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=python_task_2,
    dag=dag,
)

# Task 3: Dummy task
task_3 = DummyOperator(
    task_id='task_3',
    dag=dag,
)

# Task 4: Python task
def python_task_4():
    print("Executing python task 4")

task_4 = PythonOperator(
    task_id='task_4',
    python_callable=python_task_4,
    dag=dag,
)

# Task 5: Dummy task
task_5 = DummyOperator(
    task_id='task_5',
    dag=dag,
)

# Define task dependencies
task_1 >> task_2 >> task_3 >> task_4 >> task_5
