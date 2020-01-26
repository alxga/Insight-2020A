# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import mysql.connector
from pyspark.sql import SparkSession

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

def fetch_tpls(objKey):
  ret = []
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  gtfsrt.process_entities(data,
      eachVehiclePos=lambda x: ret.append(gtfsrt.vehpos_pb2_to_dbtpl(x))
  )
  return ret

def push_vehpos_db(keyTpls):
  cnx = None
  cursor = None
  sqlStmt = Queries["insertVehPos"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    tpls = []
    for keyTpl in keyTpls:
      tpls.append(keyTpl[1])
      if len(tpls) >= 1000:
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


if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue
  spark = builder.appName("UpdateVehPos") \
                 .getOrCreate()

  keys = fetch_keys_to_update()
  file_list = spark.sparkContext.parallelize(keys)
  records = file_list \
    .flatMap(fetch_tpls) \
    .map(lambda tpl: ((tpl[1], tpl[3]), tpl)) \
    .reduceByKey(lambda x, y: x) \
    .foreachPartition(push_vehpos_db)

  spark.stop()
