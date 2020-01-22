import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from google.protobuf.message import DecodeError
import boto3
import gtfs_realtime_pb2
from dbconn import dbConn

default_args = {
  'owner': 'Airflow',
  'depends_on_past': False,
  'start_date': datetime(2015, 6, 1),
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

_s3Bucket = "alxga-insde"
_s3ConnArgs = {
  "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
  "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
  "region_name": os.environ["AWS_REGION_NAME"]
}
s3_res = boto3.resource('s3', **_s3ConnArgs)
bucket = s3_res.Bucket(_s3Bucket)

dag = DAG('Syncs3toDB', default_args=default_args, schedule_interval=timedelta(minutes=1))

def enum_buckets_to_process():
  for obj in bucket.objects.filter(Prefix='pb/VehiclePos'):
    yield obj.key

def pb2db_vehicle_pos(pbVal):
  tStamp = datetime.fromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  return (
    pbVal.trip.route_id, tStamp.strftime('%Y-%m-%d %H:%M:%S'),
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

def process_object(objKey):
  sqlStmt = """INSERT into testair(s) values(%s);"""
  dbConn.connect()
  cursor = dbConn.cnx.cursor()
  cursor.execute(sqlStmt, (objKey,))
  dbConn.cnx.commit()
  cursor.close()
  dbConn.close()
  return

  message = gtfs_realtime_pb2.FeedMessage()
  obj = s3_res.Object(_s3Bucket, objKey)
  body = obj.get()["Body"].read()
  try:
    message.ParseFromString(body)
  except DecodeError:
    return

  sqlStmt = """
    INSERT IGNORE into TVehPos(
      RouteId, TStamp, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
    )
    values(%s, %s, %s, %s, %s, %s, %s, %s, %s);
  """
  tpls = []
  for entity in message.entity:
    # if entity.HasField('alert'):
    #   process_alert(entity.alert)
    # if entity.HasField('trip_update'):
    #   process_trip_update(entity.trip_update)
    if entity.HasField('vehicle'):
      tpls.append(pb2db_vehicle_pos(entity.vehicle))

  dbConn.connect()
  cursor = dbConn.cnx.cursor()
  cursor.executemany(sqlStmt, tpls)
  dbConn.cnx.commit()
  cursor.close()
  dbConn.close()


starter = DummyOperator(task_id='starter_vehpos2sql', dag=dag)
finisher = DummyOperator(task_id='finisher_vehpos2sql', dag=dag)
count = 0
for ObjKey in enum_buckets_to_process():
  task = PythonOperator(
      task_id='process_' + ObjKey.replace('/', '-'),
      python_callable=process_object,
      op_kwargs={'objKey': ObjKey},
      dag=dag,
  )
  task.set_upstream(starter)
  task.set_downstream(finisher)
  count += 1
  if count > 100:
    break
