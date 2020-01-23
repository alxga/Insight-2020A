# pylint: disable=unused-import

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.protobuf.message import DecodeError
import boto3
import gtfs_realtime_pb2
from dbconn import DBConn

default_args = {
  'owner': 'Airflow',
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

_s3Bucket = "alxga-insde"
_s3ConnArgs = {
  "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
  "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
  "region_name": os.environ["AWS_REGION_NAME"]
}
_s3Res = boto3.resource('s3', **_s3ConnArgs)

dag = DAG('SyncVehPos_S3toDB', default_args=default_args,
          schedule_interval=timedelta(minutes=30),
          max_active_runs=1, catchup=False)


def move_key(objKey, nObjKey):
  _s3Res.Object(_s3Bucket, nObjKey).copy_from(
    CopySource={'Bucket': _s3Bucket, 'Key': objKey}
  )
  _s3Res.Object(_s3Bucket, objKey).delete()

def move_key_to_processed(objKey):
  tkns = objKey.split('/')
  tkns[1] = "indb-" + tkns[1]
  nObjKey = '/'.join(tkns)
  move_key(objKey, nObjKey)

def move_key_from_processed(objKey):
  tkns = objKey.split('/')
  tkns[1] = tkns[1][5:]
  nObjKey = '/'.join(tkns)
  move_key(objKey, nObjKey)


def pb2db_vehicle_pos(pbVal):
  tStamp = datetime.fromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  return (
    pbVal.trip.route_id, tStamp.strftime('%Y-%m-%d %H:%M:%S'),
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

def save_obj_to_db(dbConn, objKey):
  sqlStmt = """
    INSERT IGNORE into VehPos(
      RouteId, TStamp, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
    )
    values(%s, %s, %s, %s, %s, %s, %s, %s, %s);
  """
  sqlStmtPb = """
    INSERT IGNORE into VehPosPb(name) values(%s);
  """

  message = gtfs_realtime_pb2.FeedMessage()
  obj = _s3Res.Object(_s3Bucket, objKey)
  body = obj.get()["Body"].read()
  try:
    message.ParseFromString(body)
  except DecodeError:
    return

  tpls = []
  for entity in message.entity:
    # if entity.HasField('alert'):
    #   process_alert(entity.alert)
    # if entity.HasField('trip_update'):
    #   process_trip_update(entity.trip_update)
    if entity.HasField('vehicle'):
      tpls.append(pb2db_vehicle_pos(entity.vehicle))

  cursor = dbConn.cnx.cursor()
  cursor.executemany(sqlStmt, tpls)
  cursor.execute(sqlStmtPb, (objKey,))
  dbConn.cnx.commit()
  cursor.close()


def process_all_objs():
  dbConn = DBConn()
  dbConn.connect()

  objKeys = []
  bucket = _s3Res.Bucket(_s3Bucket)
  for obj in bucket.objects.filter(Prefix='pb/VehiclePos'):
    objKeys.append(obj.key)
    if len(objKeys) > 15:
      break

  for objKey in objKeys:
    save_obj_to_db(dbConn, objKey)

  dbConn.close()



process_all_objs()

#task = PythonOperator(
#    task_id='process_all_objs',
#    python_callable=process_all_objs,
#    dag=dag
#)
