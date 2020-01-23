import os
import threading
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

NUMWRKTH = 8

_sqlCreateTempTable = """
CREATE TABLE `TempVehPos_%d` (
  `RouteId` char(50) DEFAULT NULL,
  `TStamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `VehicleId` char(50) NOT NULL,
  `TripId` char(50) NOT NULL,
  `Lat` float NOT NULL,
  `Lon` float NOT NULL,
  `Status` tinyint(4) DEFAULT NULL,
  `StopSeq` int(11) DEFAULT NULL,
  `StopId` char(50) DEFAULT NULL,
  UNIQUE KEY `unique_timetrip` (`TStamp`,`TripId`));
"""
_sqlInsertIntoTempTable = """
  INSERT IGNORE into TempVehPos_%d(
    RouteId, TStamp, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
  )
  values(%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s);
"""
_sqlMergeIntoMainTable = """
  INSERT IGNORE into VehPos
  SELECT * from TempVehPos_%d;
"""
_sqlDropTempTable = """
  DROP TABLE IF EXISTS TempVehPos_%d;
"""

_s3Bucket = "alxga-insde"
_s3ConnArgs = {
  "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
  "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
  "region_name": os.environ["AWS_REGION_NAME"]
}
s3_res = boto3.resource('s3', **_s3ConnArgs)
bucket = s3_res.Bucket(_s3Bucket)

dag = DAG('SyncVehPos_S3toDB', default_args=default_args,
          schedule_interval=timedelta(minutes=30),
          max_active_runs=1, catchup=False)


def move_key(objKey, nObjKey):
  s3_res.Object(_s3Bucket, nObjKey).copy_from(
    CopySource={'Bucket': _s3Bucket, 'Key': objKey}
  )
  s3_res.Object(_s3Bucket, objKey).delete()

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

for obj in bucket.objects.filter(Prefix='pb/indb-VehiclePos'):
  move_key_from_processed(obj.key)


def pb2db_vehicle_pos(pbVal):
  tStamp = datetime.fromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  return (
    pbVal.trip.route_id, tStamp.strftime('%Y-%m-%d %H:%M:%S'),
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

class ObjKeyList:
  def __init__(self):
    self.objKeys = []
    for obj in bucket.objects.filter(Prefix='pb/VehiclePos'):
      self.objKeys.append(obj.key)
      if len(self.objKeys) > 5000:
        break
    self.index = 0
    self.lock = threading.Lock()

  def next(self):
    with self.lock:
      ix = self.index
      self.index += 1
    if ix >= len(self.objKeys):
      return ""
    return self.objKeys[ix]

def thread_func(threadId, task_list):
  dbConn = DBConn()
  dbConn.connect()
  cursor = dbConn.cnx.cursor()

  sqlStmt = _sqlDropTempTable % threadId
  cursor.execute(sqlStmt)
  cursor.execute(_sqlCreateTempTable % threadId)
  sqlStmt = _sqlInsertIntoTempTable % threadId

  objKey = task_list.next()
  while objKey:
    message = gtfs_realtime_pb2.FeedMessage()
    obj = s3_res.Object(_s3Bucket, objKey)
    body = obj.get()["Body"].read()
    try:
      message.ParseFromString(body)

      tpls = []
      for entity in message.entity:
        # if entity.HasField('alert'):
        #   process_alert(entity.alert)
        # if entity.HasField('trip_update'):
        #   process_trip_update(entity.trip_update)
        if entity.HasField('vehicle'):
          tpls.append(pb2db_vehicle_pos(entity.vehicle))

      cursor.executemany(sqlStmt, tpls)
      dbConn.cnx.commit()

      move_key_to_processed(objKey)
    except DecodeError:
      pass
    objKey = task_list.next()

  cursor.close()
  dbConn.close()
  return 0


def process_all_objs():
  objKeyList = ObjKeyList()
  threads = []
  for threadId in range(NUMWRKTH):
    x = threading.Thread(target=thread_func, args=(threadId, objKeyList))
    threads.append(x)
    x.start()

  for th in threads:
    th.join()

  dbConn = DBConn()
  dbConn.connect()
  cursor = dbConn.cnx.cursor()
  for threadId in range(NUMWRKTH):
    sqlStmt = _sqlMergeIntoMainTable % threadId
    cursor.execute(sqlStmt)
    sqlStmt = _sqlDropTempTable % threadId
    cursor.execute(sqlStmt)
    dbConn.cnx.commit()
  cursor.close()
  dbConn.close()

process_all_objs()
task = PythonOperator(
    task_id='process_all_objs',
    python_callable=process_all_objs,
    dag=dag
)
