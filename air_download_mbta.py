# pylint: disable=unused-import

import os
import time
import threading
from datetime import datetime, timedelta
import traceback

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import download_mbta

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
  # 'queue': 'bash_queue',
  # 'pool': 'backfill',
  # 'priority_weight': 10,
  # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('DownloadMBTA', default_args=default_args,
          schedule_interval=timedelta(minutes=1),
          max_active_runs=10, catchup=False)

task = PythonOperator(
    task_id='download_mbta_main',
    python_callable=download_mbta.main,
    dag=dag
)
