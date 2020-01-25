# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import mysql.connector

from common import credentials
from common import Settings, s3, utils, gtfsrt
from queries import Queries


def fetch_keys_to_update():
  cnx = None
  cursor = None
  sqlStmt = Queries["selectVehPosPb_toAddVehPos"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    cursor.execute(sqlStmt)
    ret = []
    for tpl in cursor:
      ret.append(tpl[0])
    return ret
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()

def vehpos_pb2_to_dbtpl(pbVal):
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
  data = s3.fetch_object_body(objKey)
  gtfsrt.process_entities(data,
      eachVehiclePos=lambda x: ret.append(vehpos_pb2_to_dbtpl(x))
  )
  return ret

def push_vehpos_db(tpls):
  cnx = None
  cursor = None
  sqlStmt = Queries["insertVehPos"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
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
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue
  spark = builder.appName("PythonTestScript") \
                 .getOrCreate()

  keys = fetch_keys_to_update()
  keys = keys[0:1000]
  file_list = spark.sparkContext.parallelize(keys)
  counts = file_list \
    .flatMap(fetch_tpls) \
    .map(lambda tpl: ((tpl[1], tpl[3]), tpl)) \
    .reduceByKey(lambda x, y: x[1])

  output = counts.collect()
  for o in output:
    print(str(o))

  spark.stop()
