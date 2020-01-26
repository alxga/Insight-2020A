# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import mysql.connector
from pyspark.sql import SparkSession

from common import credentials
from common import Settings, s3, utils, gtfsrt
from queries import Queries


def fetch_vehpospb_daterange():
  cnx = None
  cursor = None
  sqlStmt = """
    SELECT min(S3KeyDT), max(S3KeyDT) FROM VehPosPb;
  """

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    cursor.execute(sqlStmt)
    return next(cursor)
  except StopIteration:
    return (None, None)
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()

def fetch_keys_after_date(mxDT):
  utcNow = datetime.utcnow()
  s3Mgr = s3.S3Mgr()
  if mxDT is None:
    ret = s3Mgr.fetch_keys("pb/VehiclePos")
  else:
    ret = []
    potentialKeys = []
    fr = datetime(mxDT.year, mxDT.month, mxDT.day)
    to = datetime(utcNow.year, utcNow.month, utcNow.day + 1)
    for dt in utils.daterange(fr, to):
      prefix = "pb/VehiclePos/%s" % dt.strftime("%Y%m%d-")
      potentialKeys += s3Mgr.fetch_keys(prefix)
    for potentialKey in potentialKeys:
      keyDT = s3.S3FeedKeyDT(potentialKey)
      if keyDT > mxDT:
        ret.append(potentialKey)
  return ret

def fetch_vehpospb_tpl(objKey):
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  return gtfsrt.vehpospb_pb2_to_dbtpl(objKey, data)

def push_vehpospb_dbtpls(tpls):
  cnx = None
  cursor = None
  sqlStmt = Queries["insertVehPosPb"]

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
  spark = builder.appName("UpdateVehPosPb") \
                 .getOrCreate()

  _, lastDT = fetch_vehpospb_daterange()
  keys = fetch_keys_after_date(lastDT)

  if len(keys) > 0:
    file_list = spark.sparkContext.parallelize(keys)
    counts = file_list \
      .map(fetch_vehpospb_tpl) \
      .foreachPartition(push_vehpospb_dbtpls)

  spark.stop()
