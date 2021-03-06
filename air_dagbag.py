"""Adds additional DAGs folders

The file should be placed into the folder $AIRFLOW_HOME/dags
"""

import os
from airflow.models import DagBag
dags_dirs = ['~/src/airdags']

for path in dags_dirs:
  dag_bag = DagBag(os.path.expanduser(path))

  if dag_bag:
    for dag_id, dag in dag_bag.dags.items():
      globals()[dag_id] = dag
