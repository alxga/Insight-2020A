# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import boto3
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import mysql.connector
from google.protobuf.message import DecodeError

from common.credentials import S3ConnArgs, MySQLConnArgs
from common import s3
from queries import Queries
import gtfs_realtime_pb2


def pb2db_vehicle_pos(pbVal):
  tStamp = datetime.utcfromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  return (
    pbVal.trip.route_id, tStamp,
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

def fetch_tpls(objKey):
  ret = []
  message = gtfs_realtime_pb2.FeedMessage()

  s3Bucket = "alxga-insde"
  s3Res = boto3.resource('s3', S3ConnArgs)

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

  keys = s3.fetch_keys("pb/VehiclePos")
  file_list = spark.sparkContext.parallelize(keys)
  counts = file_list \
    .flatMap(lambda x: [(x, fetch_tpls(x))]) \
    .map(vehpospb_row) \
    #.foreachPartition(lambda x: push_vehpospb_db(x, MySQLConnArgs))

  output = counts.collect()
  for o in output:
    print(str(o))

  spark.stop()
