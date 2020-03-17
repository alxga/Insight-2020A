"""Module to update Parquet files in S3 and the PqDate table
"""

import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types \
  import StringType, TimestampType, DoubleType, IntegerType

from common import Settings, credentials, dbtables, utils
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def fetch_parquet_dts():
  """Computes dates for which Parquet files need to be created
  """

  ret = []
  pfxDT = datetime(2020, 1, 1)
  utcNow = datetime.utcnow()
  dts = []
  # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
  while pfxDT + timedelta(days=1, hours=8, minutes=10) < utcNow:
    dts.append(pfxDT)
    pfxDT += timedelta(days=1)

  with DBConn() as conn:
    exD = dbtables.PqDates.select_existing_pqdates(conn)

  for dt in dts:
    if dt.date() not in exD:
      ret.append(dt)

  return ret


def fetch_keys_for_date(dt):
  """Retrieves Protobuf files S3 keys for a Parquet date

  Args:
    dt: target Parquet file date
  """

  with DBConn() as conn:
    # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
    dt1 = datetime(dt.year, dt.month, dt.day, 8)
    dt2 = dt1 + timedelta(days=1)
    return dbtables.VehPosPb.select_protobuf_keys_between_dates(conn, dt1, dt2)


def run(spark):
  """Updates Parquet files in S3 and the PqDate table

  Args:
    spark: Spark Session object
  """

  log = utils.get_logger()

  with DBConnCommonQueries() as conn:
    dbtables.create_if_not_exists(conn, dbtables.PqDates)

  targetDates = fetch_parquet_dts()
  for targetDate in targetDates:
    keys = fetch_keys_for_date(targetDate)
    log.info("Got %d keys of %s", len(keys), str(targetDate))

    if len(keys) > 0:
      rddKeys = spark.sparkContext \
        .parallelize(keys) \
        .map(lambda x: (x, x)) \
        .partitionBy(Settings.NumPartitions) \
        .map(lambda x: x[0])

      rddVP = rddKeys \
        .flatMap(dbtables.VehPos.build_df_tuples_from_pb) \
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
      log.info("Created dataframe for %d keys of %s", len(keys), str(targetDate))

      pqKey = targetDate.strftime("%Y%m%d")
      pqKey = '/'.join(["parquet", "VP-" + pqKey])
      pqKey = "s3a://alxga-insde/%s" % pqKey
      dfVP.write.format("parquet").mode("overwrite").save(pqKey)
      log.info("Written to Parquet %d keys of %s", len(keys), str(targetDate))
      numRecs = dfVP.count()
    else:
      numRecs = 0

    with DBConn() as conn:
      dbtables.PqDates.insert_values(conn, targetDate, len(keys), numRecs)
      conn.commit()


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
