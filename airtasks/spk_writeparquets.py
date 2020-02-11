"""Module to update Parquet files in S3 and the PqDate table
"""

import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types \
  import StringType, TimestampType, DoubleType, IntegerType

from common import credentials, s3, gtfsrt
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def fetch_parquet_dts():
  """Computes dates for which Parquet files need to be created
  """

  ret = []
  # strip time
  pfxDT = datetime(2020, 1, 1)
  utcNow = datetime.utcnow()
  dts = []
  while pfxDT + timedelta(days=1, hours=8, minutes=10) < utcNow:
    dts.append(pfxDT)
    pfxDT += timedelta(days=1)

  existing = {}
  sqlStmt = Queries["selectPqDatesWhere"] % "True"
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      existing[row[0]] = 1

  for dt in dts:
    if dt.date() not in existing:
      ret.append(dt)

  return ret


def fetch_keys_for_date(dt):
  """Retrieves Protobuf files S3 keys for a Parquet date

  Args:
    dt: target Parquet file date
  """

  sqlStmt = Queries["selectVehPosPb_forDate"]
  with DBConn() as con:
    # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
    dt1 = datetime(dt.year, dt.month, dt.day, 8)
    dt2 = dt1 + timedelta(days=1)
    cur = con.execute(sqlStmt, (dt1, dt2))
    ret = []
    for tpl in cur:
      ret.append(tpl[0])
    return ret


def fetch_tpls(objKey):
  """Retrieves vehicle position tuples from a Protobuf file

  Args:
    objKey: Protobuf file S3 key
  """

  ret = []
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  gtfsrt.process_entities(data,
      eachVehiclePos=lambda x: ret.append(gtfsrt.vehpos_pb2_to_dbtpl_dtlocal(x))
  )
  return ret


def push_pqdate(name, numKeys, numRecs):
  """Inserts a record describing a newly created Parquet file into the PqDates
  table

  Args:
    name: Parquet file date
    numKeys: number of Protobuf files processed into that Parquet file
    numRecs: number of vehicle position records in the Parquet file
  """

  sqlStmt = Queries["insertPqDate"]
  with DBConn() as con:
    con.execute(sqlStmt, (name, numKeys, numRecs))
    con.commit()


def run(spark):
  """Updates Parquet files in S3 and the PqDate table

  Args:
    spark: Spark Session object
  """

  with DBConnCommonQueries() as con:
    con.create_table("PqDates", False)

  targetDates = fetch_parquet_dts()

  for targetDate in targetDates:
    keys = fetch_keys_for_date(targetDate)
    print("Got %d keys of %s" % (len(keys), str(targetDate)), flush=True)

    if len(keys) > 0:
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
      numRecs = dfVP.count()
    else:
      numRecs = 0

    push_pqdate(targetDate, len(keys), numRecs)


if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, os.environ[envVar])
    except KeyError:
      continue

  sparkSession = builder \
    .appName("WriteParquets") \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
