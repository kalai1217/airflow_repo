from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'kalai',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dependency_example',
    default_args=default_args,
    description='A simple DAG demonstrating task dependencies',
    schedule_interval=timedelta(days=1),
)

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# Define Python functions for tasks
def task1():
    print("Executing task 1")

def task2():
    print("Executing task 2")

def task3():
    print("Executing task 3")

# Define operators for tasks
task1_operator = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)

task2_operator = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

task3_operator = PythonOperator(
    task_id='task3',
    python_callable=task3,
    dag=dag,
)

# Set up task dependencies
start_task >> task1_operator
start_task >> task2_operator
task1_operator >> task3_operator
task2_operator >> task3_operator
task3_operator >> end_task
