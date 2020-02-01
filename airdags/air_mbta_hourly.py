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

BashCmdPrefix = "cd ~/src && "


default_args = {
  'owner': 'Alex Ganin',
  'depends_on_past': False,
  'start_date': datetime(2020, 1, 1),
  'email': ['alexander_a_g@outlook.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 0,
  'max_active_runs': 1,
  'queue': 'sparks',
  # 'pool': 'backfill',
  # 'priority_weight': 10,
  # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("ProcessMBTAHourly", default_args=default_args,
          schedule_interval=timedelta(hours=1),
          max_active_runs=1, catchup=False)

update_vehpos_pb = BashOperator(
    task_id="update_vehpos_pb",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_updatevehpospb.py --on-master"),
    queue="sparks",
    dag=dag
)
update_vehpos = BashOperator(
    task_id="update_vehpos",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_updatevehpos.py --on-master"),
    queue="sparks",
    dag=dag
)
update_vehpos_pq = BashOperator(
    task_id="update_vehpos_pq",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_updatevehpospq.py --on-master"),
    queue="sparks",
    dag=dag
)
calc_vp_delays = BashOperator(
    task_id="calc_vp_delays",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_calcvpdelays.py --on-master"),
    queue="sparks",
    dag=dag
)

update_vehpos.set_upstream(update_vehpos_pb)
update_vehpos_pq.set_upstream(update_vehpos)
calc_vp_delays.set_upstream(update_vehpos_pq)
