# pylint: disable=cell-var-from-loop

import os
from datetime import date, datetime, timedelta
from collections import namedtuple

import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DoubleType, IntegerType, \
  DateType, TimestampType

from transitfeed import shapelib
from common import credentials
from common import Settings, s3, utils, gtfs
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"



class VPDelaysCalculator:
  TableSchema = StructType([
    StructField("D", DateType(), False),
    StructField("RouteId", StringType(), True),
    StructField("TripId", StringType(), False),
    StructField("StopId", StringType(), False),
    StructField("StopName", StringType(), True),
    StructField("StopLat", DoubleType(), False),
    StructField("StopLon", DoubleType(), False),
    StructField("SchedDT", TimestampType(), False),
    StructField("EstLat", DoubleType(), False),
    StructField("EstLon", DoubleType(), False),
    StructField("EstDT", TimestampType(), False),
    StructField("EstDist", DoubleType(), False),
    StructField("EstDelay", DoubleType(), False)
  ])

  VehPos = namedtuple("VehPos", "trip_id DT coords")


  def __init__(self, spark, targetDate, dfStopTimes, dfVehPos):
    self.spark = spark
    self.targetDate = targetDate
    self.dfStopTimes = dfStopTimes
    self.dfVehPos = dfVehPos


  def createResultDF(self):
    rddStopTimes = self.dfStopTimes.rdd \
      .map(lambda rec: (rec.trip_id, tuple(rec))) \
      .groupByKey()

    rddVehPos = self.dfVehPos.rdd \
      .map(lambda rec: (rec.TripId, tuple(rec))) \
      .groupByKey()

    targetDate = self.targetDate
    rddVPDelays = rddStopTimes.join(rddVehPos) \
      .flatMap(lambda keyTpl1Tpl2:
        VPDelaysCalculator._process_joined(targetDate, keyTpl1Tpl2))

    return self.spark.createDataFrame(rddVPDelays, self.TableSchema)

  @staticmethod
  def _process_joined(targetDate, keyTpl1Tpl2):
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
      vehPosLst.append(VPDelaysCalculator.VehPos(
          vehPosRec[3],
          datetime.utcfromtimestamp(vehPosRec[1].timestamp()),
          shapelib.Point.FromLatLng(vehPosRec[4], vehPosRec[5])
      ))

    return VPDelaysCalculator._calc_delays(targetDate, stopTimeLst, vehPosLst)

  @staticmethod
  def _calc_delays(targetDate, stopTimeLst, vehPosLst):
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






PqDate = namedtuple("PqDate", "Date IsInVPDelays IsInHlyDelays")


def fetch_feed_descs():
  objKey = '/'.join(["GTFS", "MBTA_archived_feeds.txt"])
  s3Mgr = s3.S3Mgr()
  content = s3Mgr.fetch_object_body(objKey)
  return gtfs.read_feed_descs(content)


def fetch_stops_df(spark, prefix):
  _objKey = '/'.join([prefix, "stops.txt"])
  _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)

  ret = spark.read.csv(_objUri, header=True)

  colNames = ["stop_id", "stop_name", "location_type", "stop_lat", "stop_lon"]
  ret = ret.select(colNames) \
    .withColumn("location_type", ret.location_type.cast(IntegerType())) \
    .withColumn("stop_lat", ret.stop_lat.cast(DoubleType())) \
    .withColumn("stop_lon", ret.stop_lon.cast(DoubleType()))
  return ret.filter("location_type = 0")


def fetch_trips_df(spark, prefix):
  _objKey = '/'.join([prefix, "trips.txt"])
  _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)

  ret = spark.read.csv(_objUri, header=True)

  colNames = ["trip_id", "route_id"]
  return ret.select(colNames)


def fetch_stop_times_df(spark, prefix):
  _objKey = '/'.join([prefix, "stop_times.txt"])
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


def read_joint_stop_times_df(spark, feedDesc):
  prefix = '/'.join(["GTFS", feedDesc.s3Key])

  # rename some columns to disambiguate after joining the tables
  stopsDF = fetch_stops_df(spark, prefix) \
    .withColumnRenamed("stop_id", "stops_stop_id")
  stopTimesDF = fetch_stop_times_df(spark, prefix)
  tripsDF = fetch_trips_df(spark, prefix) \
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

  return stopTimesDF


def fetch_pqdates_to_update():
  ret = []
  sqlStmt = Queries["selectPqDatesWhere"] %\
    "NumRecs > 0 and (not IsInVPDelays or not IsInHlyDelays)"
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      ret.append(PqDate(row[0], row[1], row[2]))
  return ret


def set_pqdate_invpdelays(D):
  sqlStmt = """
    UPDATE `PqDates` SET `IsInVPDelays` = True
    WHERE D = '%s';
  """ % D.strftime("%Y-%m-%d")
  with DBConn() as con:
    con.execute(sqlStmt)
    con.commit()


def push_vpdelays_dbtpls(rows):
  sqlStmt = Queries["insertVPDelays"]
  with DBConn() as con:
    for row in rows:
      tpl = (
        row.D, row.RouteId, row.TripId, row.StopId, row.StopName,
        row.StopLat, row.StopLon, row.SchedDT, row.EstLat, row.EstLon,
        row.EstDT, row.EstDist, row.EstDelay
      )
      print(str(tpl))
      # con.execute(sqlStmt, tpl)
      if con.uncommited % 1000 == 0:
        con.commit()
    con.commit()


def read_vp_parquet(spark, targetDate):
  objUri = "VP-" + targetDate.strftime("%Y%m%d")
  objUri = "s3a://" + '/'.join((Settings.S3BucketName, "parquet", objUri))
  return spark.read.parquet(objUri)


def run(spark):
  with DBConnCommonQueries() as con:
    con.create_table("VPDelays", False)

  feedDescs = fetch_feed_descs()
  curFeedDesc = None
  dfStopTimes = None
  feedRequiredFiles = ["stops.txt", "stop_times.txt", "trips.txt"]

  for entry in fetch_pqdates_to_update():
    targetDate = entry.Date

    if dfStopTimes is None or not curFeedDesc.includesDate(targetDate):
      curFeedDesc = None
      dfStopTimes = None
      for fd in feedDescs:
        if fd.includesDate(targetDate) and fd.includesFiles(feedRequiredFiles):
          curFeedDesc = fd
          dfStopTimes = read_joint_stop_times_df(spark, curFeedDesc)
          print('USING FEED "%s" for %s' % \
                (curFeedDesc.version, targetDate.strftime("%Y-%m-%d")))
          break
    else:
      print('RE-USING FEED "%s" for %s' % \
            (curFeedDesc.version, targetDate.strftime("%Y-%m-%d")))

    if dfStopTimes:
      dfVehPos = read_vp_parquet(spark, targetDate).limit(1000)

      calc = VPDelaysCalculator(spark, targetDate, dfStopTimes, dfVehPos)
      dfVPDelays = calc.createResultDF()

      dfVPDelays.foreachPartition(push_vpdelays_dbtpls)

      if not entry.IsInVPDelays:
        dfVPDelays.foreachPartition(push_vpdelays_dbtpls)

      #if not entry.IsInHlyDelays:
      #  hlyStopTimesRDD = stopTimesRDD \
      #    .map(lambda record: dt = dt.replace(tzinfo=pytz.timezone("US/Eastern")) \
      #    .astimezone(pytz.UTC))




if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue
  appName = "CalcVPDelays"
  sparkSession = builder \
    .appName(appName) \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
