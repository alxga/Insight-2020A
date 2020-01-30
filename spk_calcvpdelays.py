# pylint: disable=unused-import
# pylint: disable=unused-variable
# pylint: disable=cell-var-from-loop

import os
import sys
from datetime import datetime, timedelta
from collections import namedtuple

import pytz
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types \
  import StringType, TimestampType, DoubleType, IntegerType

from transitfeed import shapelib
from common import credentials
from common import Settings, s3, utils, gtfsrt
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def fetch_stops_df(spark, dt):
  _objKey = dt.strftime("%Y%m%d")
  _objKey = '/'.join(("GTFS", "MBTA_GTFS_" + _objKey, "stops.txt"))
  _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)

  ret = spark.read.csv(_objUri, header=True)

  colNames = ["stop_id", "stop_name", "location_type", "stop_lat", "stop_lon"]
  ret = ret.select(colNames) \
    .withColumn("location_type", ret.location_type.cast(IntegerType())) \
    .withColumn("stop_lat", ret.stop_lat.cast(DoubleType())) \
    .withColumn("stop_lon", ret.stop_lon.cast(DoubleType()))
  return ret.filter("location_type = 0")


def fetch_trips_df(spark, dt):
  _objKey = dt.strftime("%Y%m%d")
  _objKey = '/'.join(("GTFS", "MBTA_GTFS_" + _objKey, "trips.txt"))
  _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)

  ret = spark.read.csv(_objUri, header=True)

  colNames = ["trip_id", "route_id"]
  return ret.select(colNames)


def fetch_stop_times_df(spark, dt):
  _objKey = dt.strftime("%Y%m%d")
  _objKey = '/'.join(("GTFS", "MBTA_GTFS_" + _objKey, "stop_times.txt"))
  _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)

  _schemaStopTimes = StructType([
    StructField("trip_id", StringType(), False),
    StructField("stop_id", StringType(), False),
    StructField("time", StringType(), False),
    StructField("stop_seq", StringType(), False)
  ])
  ret = spark.read.csv(_objUri, header=True)
  ret = ret.rdd.map(lambda row: dict(
      trip_id=row.trip_id,
      stop_id=row.stop_id,
      time=row.arrival_time if row.arrival_time is not None else \
                               row.departure_time,
      stop_seq=row.stop_sequence)
  )
  ret = spark.createDataFrame(ret, schema=_schemaStopTimes)
  ret = ret.withColumn("stop_seq", ret.stop_seq.cast(IntegerType()))
  return ret.filter("time is not NULL")


def fetch_pqdts_to_update():
  ret = []
  sqlStmt = Queries["selectPqDatesWhere"] % "NumRecs > 0 and not IsInVPDelays"
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      ret.append(row[0])
  return ret


VehPos = namedtuple("VehPos", "trip_id DT coords")


def calc_delays(targetDate, stopTimeLst, vehPosLst):
  ret = []

  for stopTime in stopTimeLst:
    stopCoords = stopTime["coords"]
    stopLatLon = stopCoords.ToLatLng()

    curClosest = None
    curDist = 1e10
    for vp in vehPosLst:
      dist = stopCoords.GetDistanceMeters(vp.coords)
      if dist < curDist:
        curDist = dist
        curClosest = vp

    # adjust the scheduled time for possible date mismatches
    schedDT = stopTime["schedDT"]
    daysDiff = round((schedDT - curClosest.DT).total_seconds() / (24 * 3600))
    schedDT += timedelta(days=daysDiff)

    vpLatLon = curClosest.coords.ToLatLng()
    ret.append((
        targetDate,
        stopTime["routeId"], stopTime["tripId"], stopTime["stopId"],
        stopTime["stopName"], stopLatLon[0], stopLatLon[1], schedDT,
        vpLatLon[0], vpLatLon[1], curClosest.DT, curDist,
        (curClosest.DT - schedDT).total_seconds()
    ))

  return ret


def process_joined(targetDate, keyTpl1Tpl2):
  key = keyTpl1Tpl2[0]
  stopTimeRecs = keyTpl1Tpl2[1][0]
  vehPosRecs = keyTpl1Tpl2[1][1]

  stopTimeLst = []
  for stopTimeRec in stopTimeRecs:
    stopTimeLst.append(dict(
        tripId=stopTimeRec[0],
        stopId=stopTimeRec[1],
        stopSeq=stopTimeRec[3],
        stopName=stopTimeRec[4],
        routeId=stopTimeRec[8],
        coords=shapelib.Point.FromLatLng(stopTimeRec[6], stopTimeRec[7])
    ))
    dt = utils.sched_time_to_dt(stopTimeRec[2], targetDate)
    dt = dt.replace(tzinfo=pytz.timezone("US/Eastern")) \
      .astimezone(pytz.UTC)
    stopTimeLst[-1]["schedDT"] = dt.replace(tzinfo=None)

  vehPosLst = []
  for vehPosRec in vehPosRecs:
    vehPosLst.append(VehPos(
        vehPosRec[3],
        datetime.utcfromtimestamp(vehPosRec[1].timestamp()),
        shapelib.Point.FromLatLng(vehPosRec[4], vehPosRec[5])
    ))

  return calc_delays(targetDate, stopTimeLst, vehPosLst)


def push_vpdelays_dbtpls(tpls):
  sqlStmt = Queries["insertVPDelays"]
  with DBConn() as con:
    for tpl in tpls:
      con.execute(sqlStmt, tpl)
      if con.uncommited % 1000 == 0:
        con.commit()
    con.commit()


def set_pqdate_invpdelays(D):
  sqlStmt = """
    UPDATE `PqDates` SET `IsInVPDelays` = True
    WHERE D = '%s';
  """ % D.strftime("%Y-%m-%d")
  with DBConn() as con:
    con.execute(sqlStmt)
    con.commit()


def run(spark):
  with DBConnCommonQueries() as con:
    con.create_table("VPDelays", False)

  schedDT = datetime(2020, 1, 26)

  # rename some columns to disambiguate after joining the tables
  stopsDF = fetch_stops_df(spark, schedDT) \
    .withColumnRenamed("stop_id", "stops_stop_id")
  stopTimesDF = fetch_stop_times_df(spark, schedDT)
  tripsDF = fetch_trips_df(spark, schedDT) \
    .withColumnRenamed("trip_id", "trips_trip_id")

  stopTimesDF = stopTimesDF \
    .join(stopsDF, stopTimesDF.stop_id == stopsDF.stops_stop_id)
  stopTimesDF = stopTimesDF \
    .join(tripsDF, stopTimesDF.trip_id == tripsDF.trips_trip_id)
  colNames = [
    "trip_id", "stop_id", "time", "stop_seq", "stop_name", "location_type",
    "stop_lat", "stop_lon", "route_id"
  ]
  stopTimesDF = stopTimesDF.select(colNames)

  stopTimesRDD = stopTimesDF.rdd \
    .map(lambda rec: (rec.trip_id, tuple(rec))) \
    .groupByKey()

  targetDates = fetch_pqdts_to_update()

  for targetDate in targetDates:
    objUri = "VP-" + targetDate.strftime("%Y%m%d")
    objUri = "s3a://" + '/'.join((Settings.S3BucketName, "parquet", objUri))
    vpDF = spark.read.parquet(objUri)

    vpRDD = vpDF.rdd \
      .map(lambda rec: (rec.TripId, tuple(rec))) \
      .groupByKey()

    joinRDD = stopTimesRDD.join(vpRDD) \
      .flatMap(lambda x: process_joined(targetDate, x)) \
      .foreachPartition(push_vpdelays_dbtpls)

    set_pqdate_invpdelays(targetDate)


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
    .appName("CalcVPDelays") \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
