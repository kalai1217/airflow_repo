from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'demo_dag_with_bash',
    default_args=default_args,
    description='A simple demo DAG with BashOperator',
    schedule_interval=timedelta(days=1),
)

# Task 1: Bash command to print current date
task_1 = BashOperator(
    task_id='print_current_date',
    bash_command='date',
    dag=dag,
)

# Task 2: Bash script to list files in a directory
task_2 = BashOperator(
    task_id='list_files',
    bash_command='ls /path/to/dag',
    dag=dag,
)

# Task 3: Bash command to echo a message
task_3 = BashOperator(
    task_id='echo_message',
    bash_command='echo "This is a message"',
    dag=dag,
)

# Define task dependencies
task_1 >> task_2 >> task_3
