from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'cmattheson6',
    'depends_on_past': False,
    'email': ['cmattheson6@gmail.com'],
    'email_on_failure': False, # Eventually change to True
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

dag = DAG('house_members', default_args=default_args)

# Pipeline skeleton
# Send Pub/Sub message to activate Google Cloud Functions

t1 = BashOperator()

# Function will run and upload CSV to GCS
# Sleep 30 min

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 1800',
    retries=2,
    dag=dag
)

# Run Dataflow pipeline to process CSV and upload to BigQuery

t3 = BashOperator()

# If successful, clean out temp CSV files

t2 = BashOperator()