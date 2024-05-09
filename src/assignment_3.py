from airflow import DAG
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
    'demo_dag_with_python',
    default_args=default_args,
    description='A simple demo DAG with PythonOperator',
    schedule_interval=timedelta(days=1),
)

# Task 1: Python function to print current date
def print_current_date():
    import datetime
    print("Current date:", datetime.datetime.now())

task_1 = PythonOperator(
    task_id='print_current_date',
    python_callable=print_current_date,
    dag=dag,
)

# Task 2: Python function to perform a specific action
def perform_specific_action():
    print("Hello Everyone")

task_2 = PythonOperator(
    task_id='perform_specific_action',
    python_callable=perform_specific_action,
    dag=dag,
)

# Task 3: Python function to echo a message
def echo_message():
    print("This is a message")

task_3 = PythonOperator(
    task_id='echo_message',
    python_callable=echo_message,
    dag=dag,
)

# Define task dependencies
task_1 >> task_2 >> task_3
