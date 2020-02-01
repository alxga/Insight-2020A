from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import download_mbta_gtfsrt

__author__ = "Alex Ganin"


default_args = {
  'owner': 'Alex Ganin',
  'depends_on_past': False,
  'start_date': datetime(2020, 1, 1),
  'email': ['alexander_a_g@outlook.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 0,
  'max_active_runs': 1
}

dag = DAG('Download_MBTA_GTFSRT', default_args=default_args,
          schedule_interval=timedelta(minutes=1),
          max_active_runs=10, catchup=False)

task = PythonOperator(
    task_id='main_task',
    python_callable=download_mbta_gtfsrt.main,
    dag=dag
)
