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
  'queue': 'sparks'
}

dag = DAG("ProcessMBTAHourly", default_args=default_args,
          schedule_interval=timedelta(hours=1),
          max_active_runs=1, catchup=False)

update_vehpos_pb = BashOperator(
    task_id="index_protobufs",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_indexprotobufs.py --on-master"),
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
    task_id="write_parquets",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_writeparquets.py --on-master"),
    queue="sparks",
    dag=dag
)
calc_vp_delays = BashOperator(
    task_id="update_delays",
    bash_command=(BashCmdPrefix + "./myspark.sh spk_updatedelays.py --on-master"),
    queue="sparks",
    dag=dag
)

update_vehpos.set_upstream(update_vehpos_pb)
update_vehpos_pq.set_upstream(update_vehpos)
calc_vp_delays.set_upstream(update_vehpos_pq)
