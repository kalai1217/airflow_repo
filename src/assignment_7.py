from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'kalai',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'scheduling_example',
    default_args=default_args,
    description='A DAG demonstrating different scheduling options',
    schedule_interval=None,  # We will manually trigger this DAG
    start_date=datetime(2024, 5, 9),
)

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# Set up task dependencies
start_task >> end_task

# Example 1: Run daily at midnight
dag_daily = DAG(
    'daily_example',
    default_args=default_args,
    description='A DAG scheduled to run daily at midnight',
    schedule_interval='0 0 * * *',  # Cron expression for midnight every day
    start_date=datetime(2024, 5, 9),
)

# Define tasks for daily example
start_daily_task = DummyOperator(task_id='start', dag=dag_daily)
end_daily_task = DummyOperator(task_id='end', dag=dag_daily)

# Set up task dependencies for daily example
start_daily_task >> end_daily_task

# Example 2: Run every hour
dag_hourly = DAG(
    'hourly_example',
    default_args=default_args,
    description='A DAG scheduled to run every hour',
    schedule_interval='@hourly',  # Run every hour
    start_date=datetime(2024, 5, 9),
)

# Define tasks for hourly example
start_hourly_task = DummyOperator(task_id='start', dag=dag_hourly)
end_hourly_task = DummyOperator(task_id='end', dag=dag_hourly)

# Set up task dependencies for hourly example
start_hourly_task >> end_hourly_task

# Example 3: Run every 15 minutes
dag_quarterly = DAG(
    'quarterly_example',
    default_args=default_args,
    description='A DAG scheduled to run every 15 minutes',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    start_date=datetime(2024, 5, 9),
)

# Define tasks for quarterly example
start_quarterly_task = DummyOperator(task_id='start', dag=dag_quarterly)
end_quarterly_task = DummyOperator(task_id='end', dag=dag_quarterly)

# Set up task dependencies for quarterly example
start_quarterly_task >> end_quarterly_task
