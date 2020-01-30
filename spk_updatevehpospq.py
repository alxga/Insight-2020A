# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types \
  import StringType, TimestampType, DoubleType, IntegerType

from common import credentials, s3, utils, gtfsrt, Settings
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def fetch_keys_for_date(dt):
  sqlStmt = Queries["selectVehPosPb_forDate"]
  with DBConn() as con:
    # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
    dt1 = datetime(dt.year, dt.month, dt.day, 8)
    dt2 = datetime(dt.year, dt.month, dt.day + 1, 8)
    cur = con.execute(sqlStmt, (dt1, dt2))
    ret = []
    for tpl in cur:
      ret.append(tpl[0])
    return ret

def fetch_tpls(objKey):
  ret = []
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  gtfsrt.process_entities(data,
      eachVehiclePos=lambda x: ret.append(gtfsrt.vehpos_pb2_to_dbtpl_dtlocal(x))
  )
  return ret

def set_vehpospb_isinpq(objKeys):
  with DBConnCommonQueries() as con:
    con.set_vehpospb_flag("IsInPq", "TRUE", objKeys)


def run(spark):
  with DBConnCommonQueries() as con:
    targetDates = con.fetch_dates_to_update("NumRecs > 0 and not IsInPq")

  for targetDate in targetDates:
    keys = fetch_keys_to_update(targetDate)
    print("Got %d keys of %s" % (len(keys), str(targetDate)), flush=True)

    if len(keys) <= 0:
      continue

    rddVP = spark.sparkContext \
      .parallelize(keys) \
      .flatMap(fetch_tpls) \
      .map(lambda tpl: ((tpl[1], tpl[3]), tpl)) \
      .reduceByKey(lambda x, y: x).map(lambda x: x[1])

    schema = StructType([
      StructField("RouteId", StringType(), True),
      StructField("DT", TimestampType(), False),
      StructField("VehicleId", StringType(), False),
      StructField("TripId", StringType(), False),
      StructField("Lat", DoubleType(), False),
      StructField("Lon", DoubleType(), False),
      StructField("Status", IntegerType(), True),
      StructField("StopSeq", IntegerType(), True),
      StructField("StopId", StringType(), True),
    ])
    dfVP = spark.createDataFrame(rddVP, schema)
    print("Created dataframe for %d keys of %s" % (len(keys), str(targetDate)), flush=True)

    pqKey = targetDate.strftime("%Y%m%d")
    pqKey = '/'.join(["parquet", "VP-" + pqKey])
    pqKey = "s3a://alxga-insde/%s" % pqKey
    dfVP.write.format("parquet").mode("overwrite").save(pqKey)
    print("Written to Parquet %d keys of %s" % (len(keys), str(targetDate)), flush=True)

    print("Updating the VehPosPb table %s" % pqKey)
    spark.sparkContext \
      .parallelize(keys) \
      .foreachPartition(set_vehpospb_isinpq)
    print("Updated IsInPq for %d keys of %s" % (len(keys), str(targetDate)), flush=True)


if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, os.environ[envVar])
    except KeyError:
      continue

  sparkSession = builder \
    .appName("MakeParquets") \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
