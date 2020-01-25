# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import boto3
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import mysql.connector

from common import credentials
from common import Settings, s3, gtfsrt
from queries import Queries


def fetch_vehpospb_tpl(objKey):
  data = s3.fetch_object_body(objKey)
  dts = []

  gtfsrt.process_entities(data,
      eachVehiclePos=lambda x:
          dts.append(datetime.utcfromtimestamp(x.timestamp))
  )

  kdt = s3.S3FeedKeyDT(objKey)
  mn = min(dts, default=None)
  mx = max(dts, default=None)
  return (objKey, len(dts), kdt, mn, mx)

def push_vehpospb_db(tpls):
  sqlStmt = Queries["insertVehPosPb"]

  cnx = None
  cursor = None
  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    # cnx.close()
    # return
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

  keys = s3.fetch_keys("pb/VehiclePos")
  for k in keys:
    print(s3.S3FeedKeyDT(k))

  file_list = spark.sparkContext.parallelize(keys)
  counts = file_list \
    .map(fetch_vehpospb_tpl) \
    .foreachPartition(lambda x: push_vehpospb_db(x))

  output = counts.collect()
  for o in output:
    print(str(o))

  spark.stop()
