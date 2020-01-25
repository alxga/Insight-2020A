# pylint: disable=unused-import

import sys
sys.path.insert(0, 'mysparkreqs.zip')

import os
from operator import add
from datetime import datetime, timedelta

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import mysql.connector
import boto3
from google.protobuf.message import DecodeError
import gtfs_realtime_pb2
from queries import Queries


_s3ConnArgs = {
  "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
  "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
  "region_name": os.environ["AWS_DEFAULT_REGION"]
}
_mysqlConnArgs = {
  "user": os.environ['MYSQL_USER'],
  "password": os.environ['MYSQL_PWD'],
  "host": os.environ['MYSQL_HOST'],
  "database": os.environ['MYSQL_DBNAME']
}

def fetch_keys():
  s3Bucket = "alxga-insde"
  s3Res = boto3.resource('s3')

  objKeys = []
  bucket = s3Res.Bucket(s3Bucket)
  for obj in bucket.objects.filter(Prefix='pb/VehiclePos'):
    objKeys.append(obj.key)
    if len(objKeys) > 100:
      break
  return objKeys


def pb2db_vehicle_pos(pbVal):
  tStamp = datetime.utcfromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  return (
    pbVal.trip.route_id, tStamp,
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

def fetch_tpls(objKey, s3ConnArgs):
  ret = []
  message = gtfs_realtime_pb2.FeedMessage()

  s3Bucket = "alxga-insde"
  s3Res = boto3.resource('s3', **s3ConnArgs)

  obj = s3Res.Object(s3Bucket, objKey)
  body = obj.get()["Body"].read()
  try:
    message.ParseFromString(body)
  except DecodeError:
    return ret

  for entity in message.entity:
    # if entity.HasField('alert'):
    #   process_alert(entity.alert)
    # if entity.HasField('trip_update'):
    #   process_trip_update(entity.trip_update)
    if entity.HasField('vehicle'):
      ret.append(pb2db_vehicle_pos(entity.vehicle))
  return ret

def vehpospb_row(key_tpls):
  k = key_tpls[0]
  tpls = key_tpls[1]
  l = len(tpls)
  mn = min([tpl[1] for tpl in tpls], default=None)
  mx = max([tpl[1] for tpl in tpls], default=None)
  return (k, l, mn, mx)

def push_vehpospb_db(tpls, mysqlConnArgs):
  sqlStmt = Queries["insertVehPosPb"]

  cnx = None
  cursor = None
  try:
    cnx = mysql.connector.connect(**mysqlConnArgs)
    cursor = cnx.cursor()
    count = 0
    for tpl in tpls:
      cursor.execute(sqlStmt, tpl)
      count += 1
      if count % 1000 == 0:
        cnx.commit()
    cnx.commit()
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()
  


if __name__ == "__main__":
  spark = SparkSession\
      .builder\
      .appName("PythonTestScript")\
      .getOrCreate()


  keys = fetch_keys()
  file_list = spark.sparkContext.parallelize(keys)
  counts = file_list \
    .flatMap(lambda x: [(x, fetch_tpls(x, _s3ConnArgs))]) \
    .map(vehpospb_row) \
    .foreachPartition(lambda x: push_vehpospb_db(x, _mysqlConnArgs))

  #output = counts.collect()
  #for o in output:
  #  print(str(o))

  spark.stop()
