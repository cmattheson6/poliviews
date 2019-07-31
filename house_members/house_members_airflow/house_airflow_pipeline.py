from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
# import house_members.house_members_scrapy.main as scrape
# import house_members.house_members_dataflow as df
# import os

gcs_creds = 'C:/Users/cmatt/Downloads/gce_creds.json'
project_id = 'politics-data-tracker-1'
bucket_name = 'poliviews'
pipeline_name = 'house_members'
blob_name = 'csvs/{0}/{0}_{1}.csv'.format(pipeline_name, date.today())
gcs_path = 'gs://' + bucket_name + '/' + blob_name

default_args = {
    'owner': 'cmattheson6',
    'depends_on_past': False,
    'email': ['cmattheson6@gmail.com'],
    'email_on_failure': False, # Eventually change to True
    'email_on_retry': False,
    'start_date': datetime(2019, 6, 30),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('house_members',
          default_args=default_args,
          schedule_interval='@daily')

# Pipeline skeleton
# Send Pub/Sub message to activate Google Cloud Functions

# THIS IS NOT AT ALL WHAT I NEED HERE; FIGURE OUT HOW TO SEND A PUB/SUB MESSAGE
# t1 = PythonOperator(
#     task_id='scrape_house_members',
#     python_callable=scrape.main,
#     retries=2,
#     dag=dag
# )

t1 = BashOperator(
    task_id='scrape_house_members',
    bash_command='gcloud pubsub topics publish scrapy --message "house members scrape"',
    retries=2,
    dag=dag
)

# Function will run and upload CSV to GCS
# Sleep 30 min

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 30m',
    retries=2,
    dag=dag
)

# Run Dataflow pipeline to process CSV and upload to BigQuery

# t3 = PythonOperator(
#     task_id='house_members_df',
#     python_callable=df.house_members_df.main(), ### Needs to change, need --setup_file= and --experiments=allow_non_updatable_job_parameter
#     retries=2,
#     dag=dag
# )

# t3 = BashOperator(
#     task_id='df_v2',
#     bash_command='cd ../house_members_dataflow \
#     py -m house_members_df --setup_file=./setup.py --experiments=allow_non_updatable_job_parameter',
#     dag=dag
# )

t3 = DataFlowPythonOperator(
    task_id='df_v3',
    py_file='../house_members_dataflow/house_members_df.py',
    py_options=['--setup_file={0}'.format('../house_members_dataflow/setup.py'),
                '--experiments=allow_non_updatable_job_parameter'],
    dag=dag
)

# If successful, clean out temp CSV files

t4 = BashOperator(
    task_id='clean_up',
    bash_command='gsutil rm {0}'.format(gcs_path),
    dag=dag
)

# Build task pipeline order

t1 >> t2 >> t3 >> t4