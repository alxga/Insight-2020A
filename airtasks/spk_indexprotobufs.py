"""Module to update the metadata about S3 files in S3Prefixes and VehPosPb
tables
"""

import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from common import credentials
from common import s3, gtfsrt
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def fetch_s3prefixes():
  """Computes a list of S3 prefixes under which to check for new
  Protobuf files

  A prefix is a combination of date and an hour when the Protobufs
  were downloaded. Each hour starting on 2020/01/01 00:00 and
  ending 70 minutes before the current time is returned unless it's marked
  in the S3Prefixes table as processed.
  """

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

  for pfx in pfxs:
    if pfx not in existing:
      ret.append(pfx)

  return ret


def create_vehpospb_tpl(objKey):
  """Creates a record to add to the VehPosPb table for a Protobuf file

  Args:
    objKey: S3 key of the Protobuf file
  """
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  return gtfsrt.vehpospb_pb2_to_dbtpl(objKey, data)


def push_vehpospb_dbtpls(tpls):
  """Pushes records to the VehPosPb table

  Args:
    tpls: records to add, each contains metadata for a single Protobuf file
  """
  sqlStmt = Queries["insertVehPosPb"]
  with DBConn() as con:
    for tpl in tpls:
      con.execute(sqlStmt, tpl)
      if con.uncommited % 1000 == 0:
        con.commit()
    con.commit()

def push_s3prefix(name, numKeys):
  """Pushes a record to the S3Prefixes table

  Args:
    name: an S3 prefix, e.g., '20200115/10'
    numKeys: number of Protobuf files under this prefix
  """
  sqlStmt = Queries["insertS3Prefix"]
  with DBConn() as con:
    con.execute(sqlStmt, (name, numKeys))
    con.commit()


def run(spark):
  """Indexes Protobuf files by updating the S3Prefixes and VehPosPb tables

  Args:
    spark: Spark Session object
  """
  with DBConnCommonQueries() as con:
    con.create_table("S3Prefixes", False)

  pfxs = fetch_s3prefixes()
  s3Mgr = s3.S3Mgr()
  for pfx in pfxs:
    fullPfx = '/'.join(("pb", "VehiclePos", pfx))
    keys = s3Mgr.fetch_keys(fullPfx)
    if len(keys) > 0:
      print("PROCESSING %d KEYS FOR %s" % (len(keys), pfx))
      file_list = spark.sparkContext.parallelize(keys)
      file_list \
        .map(create_vehpospb_tpl) \
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
    .appName("IndexProtobufs") \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
