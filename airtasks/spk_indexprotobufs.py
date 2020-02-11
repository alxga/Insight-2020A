"""Module to update the metadata about S3 files in S3Prefixes and VehPosPb
tables
"""

import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from common import credentials
from common import s3, dbtables
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def explore_s3prefixes():
  """Computes a list of S3 prefixes under which to check for new
  Protobuf files

  A prefix is a combination of date and an hour when the Protobufs
  were downloaded. Each hour starting on 2020/01/01 00:00 and
  ending 70 minutes before the current time is returned unless it's marked
  in the S3Prefixes table as processed.
  """

  ret = []
  pfxDT = datetime(2020, 1, 1)
  utcNow = datetime.utcnow()
  pfxs = []
  while pfxDT + timedelta(hours=1, minutes=10) < utcNow:
    pfxs.append(pfxDT.strftime("%Y%m%d/%H"))
    pfxDT += timedelta(hours=1)

  with DBConn() as conn:
    exPrefixes = dbtables.S3Prefixes.selectPrefixesDict(conn)

  for pfx in pfxs:
    if pfx not in exPrefixes:
      ret.append(pfx)

  return ret


def push_vehpospb_dbtpls(tpls):
  """Pushes records to the VehPosPb table

  Args:
    tpls: records to add, each contains metadata for a single Protobuf file
  """

  with DBConn() as conn:
    for tpl in tpls:
      dbtables.VehPosPb.insertTpl(conn, tpl)
      if conn.uncommited % 1000 == 0:
        conn.commit()
    conn.commit()


def run(spark):
  """Indexes Protobuf files by updating the S3Prefixes and VehPosPb tables

  Args:
    spark: Spark Session object
  """

  with DBConnCommonQueries() as conn:
    dbtables.create_if_not_exists(conn, dbtables.S3Prefixes)
    dbtables.create_if_not_exists(conn, dbtables.VehPosPb)

  pfxs = explore_s3prefixes()
  s3Mgr = s3.S3Mgr()
  for pfx in pfxs:
    fullPfx = '/'.join(("pb", "VehiclePos", pfx))
    keys = s3Mgr.fetch_keys(fullPfx)
    if len(keys) > 0:
      print("PROCESSING %d KEYS FOR %s" % (len(keys), pfx))
      file_list = spark.sparkContext.parallelize(keys)
      file_list \
        .map(dbtables.VehPosPb.buildTupleFromProtobuf) \
        .foreachPartition(push_vehpospb_dbtpls)
      print("PROCESSED %d KEYS FOR %s" % (len(keys), pfx))
    tpl = (pfx, len(keys))

    with DBConn() as conn:
      dbtables.S3Prefixes.insertValues(conn, pfx, len(keys))
      conn.commit()
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
    .appName("IndexProtobufs") \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
