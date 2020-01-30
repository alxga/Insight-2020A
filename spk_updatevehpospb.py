# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import mysql.connector
from pyspark.sql import SparkSession

from common import credentials
from common import Settings, s3, utils, gtfsrt
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def fetch_s3prefixes():
  ret = []
  # strip time
  pfxDT = datetime(2020, 1, 1)
  utcNow = datetime.utcnow()
  pfxs = []
  while pfxDT + timedelta(hours=1, minutes=10) < utcNow:
    pfxs.append(pfxDT.strftime("%Y%m%d/%H"))
    pfxDT += timedelta(hours=1)

  existing = {}
  sqlStmt = Queries["selectS3Prefixes"]
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      existing[row[0]] = 1

  for pfx in ret:
    if pfx not in existing:
      ret.append(pfx)

  return ret


def fetch_vehpospb_tpl(objKey):
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  return gtfsrt.vehpospb_pb2_to_dbtpl(objKey, data)


def push_vehpospb_dbtpls(tpls):
  sqlStmt = Queries["insertVehPosPb"]
  with DBConn() as con:
    for tpl in tpls:
      con.execute(sqlStmt, tpl)
      if con.uncommited % 1000 == 0:
        con.commit()
    con.commit()

def push_s3prefix(name, numKeys):
  sqlStmt = Queries["insertS3Prefix"]
  with DBConn() as con:
    con.execute(sqlStmt, (name, numKeys))
    con.commit()


def run(spark):
  with DBConnCommonQueries() as con:
    con.create_table("S3Prefixes", False)

  pfxs = fetch_s3prefixes()
  s3Mgr = s3.S3Mgr()
  for pfx in pfxs:
    keys = s3Mgr.fetch_keys(pfx)
    if len(keys) > 0:
      print("PROCESSING %d KEYS FOR %s" % (len(keys), pfx))
      file_list = spark.sparkContext.parallelize(keys)
      file_list \
        .map(fetch_vehpospb_tpl) \
        .foreachPartition(push_vehpospb_dbtpls)
      print("PROCESSED %d KEYS FOR %s" % (len(keys), pfx))
    tpl = (pfx, len(keys))
    push_s3prefix(*tpl)
    print("PUSHED S3Prefix %s" % str(tpl))


if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue

  sparkSession = builder \
    .appName("UpdateVehPosPb") \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
