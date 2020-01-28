# pylint: disable=unused-import
# pylint: disable=unused-variable

import os
import sys
from datetime import datetime, timedelta
from collections import namedtuple

import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types \
  import StringType, TimestampType, DoubleType, IntegerType

from common import credentials
from common import Settings, s3, utils, gtfsrt, queryutils, geo
from common.queries import Queries

__author__ = "Alex Ganin"


def fetch_stop_times_df(sparkSession, dt):
  _objKey = dt.strftime("%Y%m%d")
  _objKey = '/'.join(("GTFS", "MBTA_GTFS_" + _objKey, "stop_times.txt"))
  _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)
  _schemaStopTimes = StructType([
    StructField("trip_id", StringType(), False),
    StructField("arrival_time", StringType(), False),
    StructField("departure_time", StringType(), False),
    StructField("stop_id", StringType(), False),
    StructField("stop_sequence", StringType(), False),
    StructField("stop_headsign", StringType(), True),
    StructField("pickup_type", IntegerType(), True),
    StructField("drop_off_type", IntegerType(), True),
    StructField("timepoint", StringType(), True),
    StructField("checkpoint_id", StringType(), True)
  ])
  ret = sparkSession.read \
    .schema(_schemaStopTimes) \
    .option("mode", "DROPMALFORMED") \
    .csv(_objUri, header=True)
  return ret


StopTime = namedtuple("StopTime", "trip_id stop_id timeStr")
VehPos = namedtuple("VehPos", "trip_id tstamp coords")

def process_joined(keyTpl1Tpl2):
  key = keyTpl1Tpl2[0]
  stopTimeRecs = keyTpl1Tpl2[1][0]
  vehPosRecs = keyTpl1Tpl2[1][1]

  stopTimeLst = []
  for stopTimeRec in stopTimeRecs:
    stopTimeLst.append(StopTime(
        stopTimeRec[0], stopTimeRec[3], stopTimeRec[1],
    ))
  vehPosLst = []
  for vehPosRec in vehPosRecs:
    vehPosLst.append(VehPos(
        vehPosRec[3], vehPosRec[1],
        geo.Coords(vehPosRec[4], vehPosRec[5])
    ))

  print("FOREACH")
  print("Got %d stop_times for trip %s" % (len(stopTimeLst), key))
  print("Got %d vehicle positions for trip %s" % (len(vehPosLst), key))







if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue
  spark = builder.appName("CalcVPDelays") \
                 .getOrCreate()

  schedDT = datetime(2020, 1, 19)
  stopTimesDF = fetch_stop_times_df(spark, schedDT)
  stopTimesRDD = stopTimesDF.rdd \
    .map(lambda rec: (rec.trip_id, tuple(rec))) \
    .groupByKey()

  targetDates = queryutils.fetch_dates_to_update()

  for targetDate in targetDates:
    targetDT = datetime(targetDate.year, targetDate.month, targetDate.day)
    objUri = "VP-" + targetDT.strftime("%Y%m%d")
    objUri = "s3a://" + '/'.join((Settings.S3BucketName, "parquet", objUri))
    vpDF = spark.read.parquet(objUri)

    vpRDD = vpDF.rdd \
      .map(lambda rec: (rec.TripId, tuple(rec))) \
      .groupByKey()

    joinRDD = stopTimesRDD.join(vpRDD) \
      .foreach(process_joined)

  spark.stop()

