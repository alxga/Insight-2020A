"""Airflow DAG to download MBTA schedule if it's updated"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airtasks import download_mbta_gtfs

__author__ = "Alex Ganin"


default_args = {
  'owner': 'Alex Ganin',
  'depends_on_past': False,
  'start_date': datetime(2020, 1, 1),
  'email': ['alexander_a_g@outlook.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 5,
  'retry_delay': timedelta(hours=3),
  'max_active_runs': 1
}

dag = DAG('Download_MBTA_GTFS', default_args=default_args,
          schedule_interval=timedelta(days=1),
          max_active_runs=1, catchup=False)

task = PythonOperator(
    task_id='main_task',
    python_callable=download_mbta_gtfs.main,
    dag=dag
)
