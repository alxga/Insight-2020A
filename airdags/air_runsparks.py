# pylint: disable=unused-import

import os
import time
import threading
from datetime import datetime, timedelta
import traceback

import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

__author__ = "Alex Ganin"

BashCmdPrefix = "~/src & "


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

dag = DAG("ProcessData", default_args=default_args,
          schedule_interval=timedelta(hours=1),
          max_active_runs=1, catchup=False)

task = BashOperator(
    task_id="update_vehpos_pb",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_updatevehpospb.py --on-master"),
    queue="sparks",
    dag=dag
)
