from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from Weather_Data_Generator_DAG import generate_weather_data
# from Weather_Data_Generator_DAG import get_weather_conditions

import json
import random
import boto3
import uuid 


default_arguments={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2024,11,8),
    'email':['example@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}


dag = DAG(
     'Weather_Data_Generation_DAG',
     default_args=default_arguments,
     description='Generate Weather Data',
     schedule_interval='*/1 * * * *',  # Run every 1 minute
     ##schedule_interval='schedule_interval',  # This ensures the DAG runs ONLY when triggered manually
     catchup=False,  # Prevents running past missed intervals
 )


run_etl = PythonOperator(
    task_id='Data_Genearation',
    python_callable=generate_weather_data,
    dag=dag,
)

run_etl