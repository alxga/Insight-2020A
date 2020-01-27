# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import mysql.connector
from pyspark.sql import SparkSession

from common import credentials
from common import Settings, s3, utils, gtfsrt
from queries import Queries


def recreate_table_tu():
  cnx = None
  cursor = None
  sqlStmt = Queries["createTU"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    cursor.execute("DROP TABLE IF EXISTS `TU`;")
    cursor.execute(sqlStmt)
    cnx.commit()
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()


def fetch_keys():
  s3Mgr = s3.S3Mgr()
  return s3Mgr.fetch_keys("pb/TripUpdates", 30)


def tu_pb2_to_dbtpls(lst, objKey, pbVal):
  s3KeyDT = s3.S3FeedKeyDT(objKey)
  startDateStr = pbVal.trip.start_date
  startDateStr = '-'.join((startDateStr[0:4],
                           startDateStr[4:6],
                           startDateStr[6:8]))
  for stopTime in pbVal.stop_time_update:
    if stopTime.HasField('arrival'):
      arrDT = datetime.utcfromtimestamp(stopTime.arrival.time)
    else:
      arrDT = None
    if stopTime.HasField('departure'):
      depDT = datetime.utcfromtimestamp(stopTime.departure.time)
    else:
      depDT = None
    if not arrDT and not depDT:
      continue
    tpl = (
      s3KeyDT, pbVal.trip.trip_id, startDateStr, stopTime.stop_id,
      stopTime.stop_sequence, arrDT, depDT
    )
    lst.append(tpl)


def fetch_tu_tpls(objKey):
  ret = []
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  gtfsrt.process_entities(data,
      eachTripUpdate=lambda x: tu_pb2_to_dbtpls(ret, objKey, x)
  )
  return ret


def push_tu_db(allTpls):
  cnx = None
  cursor = None
  sqlStmt = Queries["insertTU"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    tpls = []
    for tpl in allTpls:
      tpls.append(tpl)
      if len(tpls) >= 100:
        cursor.executemany(sqlStmt, tpls)
        cnx.commit()
        tpls = []
    if len(tpls) > 0:
      cursor.executemany(sqlStmt, tpls)
      cnx.commit()
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()



recreate_table_tu()

if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue
  spark = builder.appName("UpdateTU") \
                 .getOrCreate()

  keys = fetch_keys()
  recs = []
  for key in keys:
    recs += fetch_tu_tpls(key)
  print("Length is %d" % len(recs))
  push_tu_db(recs)

  if len(keys) > 0:
    res = spark.sparkContext.parallelize(keys)
    res = res.flatMap(fetch_tu_tpls)

    collected = res.collect()
    print("Read %d tuples" % len(collected))

    res.foreachPartition(push_tu_db)

  spark.stop()
