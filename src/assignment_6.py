from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sensor_example',
    default_args=default_args,
    description='A DAG demonstrating the usage of different types of sensors',
    schedule_interval=timedelta(days=1),
)

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# Define sensors
file_sensor = FileSensor(
    task_id='file_sensor',
    filepath='/path/to/your/file.txt',
    poke_interval=30,  # Check for file existence every 30 seconds
    timeout=600,  # Timeout if file doesn't exist after 10 minutes
    dag=dag,
)

http_sensor = HttpSensor(
    task_id='http_sensor',
    method='GET',
    http_conn_id='http_default',  # Connection ID configured in Airflow UI
    endpoint='/your/api/endpoint',
    request_params={},  # Additional request parameters if required
    response_check=lambda response: True if response.status_code == 200 else False,  # Custom response check function
    poke_interval=60,  # Check HTTP endpoint every 60 seconds
    timeout=600,  # Timeout if endpoint doesn't return expected response after 10 minutes
    dag=dag,
)

time_sensor = TimeSensor(
    task_id='time_sensor',
    target_time=datetime(2024, 5, 10, 8, 0),  # Wait until May 10, 2024, 08:00 AM
    timeout=600,  # Timeout if time is reached after 10 minutes
    poke_interval=60,  # Check time every 60 seconds
    dag=dag,
)

# Set up task dependencies
start_task >> [file_sensor, http_sensor, time_sensor] >> end_task
