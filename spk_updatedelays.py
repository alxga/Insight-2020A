# pylint: disable=cell-var-from-loop

import os
from datetime import datetime, timedelta
from collections import namedtuple

import pytz
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DoubleType, IntegerType, \
  DateType, TimestampType
from third_party.transitfeed import shapelib
from common import credentials
from common import Settings, s3, utils, gtfs
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


class VPDelaysCalculator:
  TableSchema = StructType([
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


  def __init__(self, spark, pqDate, dfStopTimes, dfVehPos):
    self.spark = spark
    self.pqDate = pqDate
    self.dfStopTimes = dfStopTimes
    self.dfVehPos = dfVehPos


  def createResultDF(self):
    rddStopTimes = self.dfStopTimes.rdd \
      .map(lambda rec: (rec.trip_id, tuple(rec))) \
      .groupByKey()

    rddVehPos = self.dfVehPos.rdd \
      .map(lambda rec: (rec.TripId, tuple(rec))) \
      .groupByKey()

    pqDate = self.pqDate
    rddVPDelays = rddStopTimes.join(rddVehPos) \
      .flatMap(lambda keyTpl1Tpl2:
        VPDelaysCalculator._process_joined(pqDate, keyTpl1Tpl2))

    return self.spark.createDataFrame(rddVPDelays, self.TableSchema)


  def updateDB(self, dfVPDelays):
    pqDate = self.pqDate
    dfVPDelays.foreachPartition(lambda rows:
        VPDelaysCalculator._push_vpdelays_dbtpls(rows, pqDate)
    )


  @staticmethod
  def _process_joined(pqDate, keyTpl1Tpl2):
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
      dt = utils.sched_time_to_dt(stopTimeRec[2], pqDate)
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

    return VPDelaysCalculator._calc_delays(stopTimeLst, vehPosLst)

  @staticmethod
  def _calc_delays(stopTimeLst, vehPosLst):
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
          stopTime["routeId"], stopTime["tripId"], stopTime["stopId"],
          stopTime["stopName"], stopLatLon[0], stopLatLon[1], schedDT,
          vpLatLon[0], vpLatLon[1], curClosest.DT, curDist,
          (curClosest.DT - schedDT).total_seconds()
      ))
    return ret

  @staticmethod
  def _push_vpdelays_dbtpls(rows, pqDate):
    sqlStmt = Queries["insertVPDelays"]
    with DBConn() as con:
      for row in rows:
        tpl = (
          pqDate, row.RouteId, row.TripId, row.StopId, row.StopName,
          row.StopLat, row.StopLon, row.SchedDT, row.EstLat, row.EstLon,
          row.EstDT, row.EstDist, row.EstDelay
        )
        con.execute(sqlStmt, tpl)
        if con.uncommited % 1000 == 0:
          con.commit()
      con.commit()


class HlyDelaysCalculator:
  TableSchema = StructType([
    StructField("DateEST", DateType(), False),
    StructField("HourEST", IntegerType(), False),
    StructField("RouteId", StringType(), True),
    StructField("StopName", StringType(), True),
    StructField("AvgDelay", DoubleType(), False),
    StructField("AvgDist", DoubleType(), False),
    StructField("Cnt", IntegerType(), False),
    StructField("StopLat", DoubleType(), True),
    StructField("StopLon", DoubleType(), True),
    StructField("StopId", StringType(), True)
  ])

  DateHour = StructType([
    StructField("DateEST", DateType(), False),
    StructField("HourEST", IntegerType(), False)
  ])

  @staticmethod
  def datetime_to_datehour(dt):
    dt = dt.replace(tzinfo=pytz.UTC) \
        .astimezone(pytz.timezone("US/Eastern"))
    return (dt.date(), dt.hour)

  def __init__(self, spark, dfVPDelays):
    self.spark = spark
    self.dfVPDelays = dfVPDelays

  def createResultDF(self):
    udf_datetime_to_datehour = F.udf(
      HlyDelaysCalculator.datetime_to_datehour,
      HlyDelaysCalculator.DateHour
    )
    dfResult = self.dfVPDelays \
      .withColumn(
          "datehour",
          udf_datetime_to_datehour(self.dfVPDelays.SchedDT)
      )
    dfResult = dfResult.filter("EstDist < 100")
    dfResult = dfResult \
      .withColumn("DateEST", dfResult.datehour.DateEST) \
      .withColumn("HourEST", dfResult.datehour.HourEST) \
      .drop("datehour")
    dfResult = dfResult \
      .groupBy(
          dfResult.DateEST, dfResult.HourEST,
          dfResult.RouteId, dfResult.StopName
      ) \
      .agg(
            F.mean(dfResult.EstDelay).alias("AvgDelay"),
            F.mean(dfResult.EstDist).alias("AvgDist"),
            F.count(F.lit(1)).alias("Cnt"),
            F.first(dfResult.StopLat).alias("StopLat"),
            F.first(dfResult.StopLon).alias("StopLon"),
            F.first(dfResult.StopId).alias("StopId")
      )

    return dfResult

  def groupRoutes(self, dfHlyDelays):
    dfResult = dfHlyDelays \
      .groupBy(
          dfHlyDelays.DateEST, dfHlyDelays.HourEST, dfHlyDelays.RouteId
      ) \
      .agg(
          (F.sum(dfHlyDelays.AvgDelay * dfHlyDelays.Cnt) /
           F.sum(dfHlyDelays.Cnt)).alias("AvgDelay"),
          (F.sum(dfHlyDelays.AvgDist * dfHlyDelays.Cnt) /
           F.sum(dfHlyDelays.Cnt)).alias("AvgDist"),
          F.sum(dfHlyDelays.Cnt).alias("Cnt"))
    return dfResult

  def groupStops(self, dfHlyDelays):
    dfResult = dfHlyDelays \
      .groupBy(
          dfHlyDelays.DateEST, dfHlyDelays.HourEST, dfHlyDelays.StopName
      ) \
      .agg(
          (F.sum(dfHlyDelays.AvgDelay * dfHlyDelays.Cnt) /
           F.sum(dfHlyDelays.Cnt)).alias("AvgDelay"),
          (F.sum(dfHlyDelays.AvgDist * dfHlyDelays.Cnt) /
           F.sum(dfHlyDelays.Cnt)).alias("AvgDist"),
          F.sum(dfHlyDelays.Cnt).alias("Cnt"),
          F.first(dfHlyDelays.StopLat).alias("StopLat"),
          F.first(dfHlyDelays.StopLon).alias("StopLon"),
          F.first(dfHlyDelays.StopId).alias("StopId")
      )
    return dfResult

  def groupAll(self, dfHlyDelays):
    dfResult = dfHlyDelays \
      .groupBy(
          dfHlyDelays.DateEST, dfHlyDelays.HourEST
      ) \
      .agg(
          (F.sum(dfHlyDelays.AvgDelay * dfHlyDelays.Cnt) /
           F.sum(dfHlyDelays.Cnt)).alias("AvgDelay"),
          (F.sum(dfHlyDelays.AvgDist * dfHlyDelays.Cnt) /
           F.sum(dfHlyDelays.Cnt)).alias("AvgDist"),
          F.sum(dfHlyDelays.Cnt).alias("Cnt")
      )
    return dfResult


  def updateDB(self, dfHlyDelays, pqDate, noRouteVal=None):
    dfHlyDelays.foreachPartition(lambda rows:
        HlyDelaysCalculator._push_hlydelays_dbtpls(rows, pqDate, noRouteVal)
    )

  @staticmethod
  def _push_hlydelays_dbtpls(rows, pqDate, noRouteVal):
    sqlStmt = Queries["insertHlyDelays"]
    with DBConn() as con:
      for row in rows:
        tpl = (
          pqDate,
          row.DateEST, row.HourEST,
          getattr(row, "RouteId", noRouteVal),
          getattr(row, "StopName", None),
          row.AvgDelay, row.AvgDist, row.Cnt,
          getattr(row, "StopLat", None),
          getattr(row, "StopLon", None),
          getattr(row, "StopId", None)
        )
        con.execute(sqlStmt, tpl)
        if con.uncommited % 1000 == 0:
          con.commit()
      con.commit()


class GTFSFetcher:
  def __init__(self, spark):
    self.spark = spark

  def _fetch_stops_df(self, prefix):
    _objKey = '/'.join([prefix, "stops.txt"])
    _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)
    dfStops = self.spark.read.csv(_objUri, header=True)
    colNames = ["stop_id", "stop_name", "location_type", "stop_lat", "stop_lon"]
    dfStops = dfStops.select(colNames) \
      .withColumn("location_type", dfStops.location_type.cast(IntegerType())) \
      .withColumn("stop_lat", dfStops.stop_lat.cast(DoubleType())) \
      .withColumn("stop_lon", dfStops.stop_lon.cast(DoubleType()))
    return dfStops.filter("location_type = 0")

  def _fetch_trips_df(self, prefix):
    _objKey = '/'.join([prefix, "trips.txt"])
    _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)
    dfTrips = self.spark.read.csv(_objUri, header=True)
    colNames = ["trip_id", "route_id"]
    return dfTrips.select(colNames)

  def _fetch_stop_times_df(self, prefix):
    _objKey = '/'.join([prefix, "stop_times.txt"])
    _objUri = "s3a://%s/%s" % (Settings.S3BucketName, _objKey)
    _schemaStopTimes = StructType([
      StructField("trip_id", StringType(), False),
      StructField("stop_id", StringType(), False),
      StructField("time", StringType(), False),
      StructField("stop_seq", StringType(), False)
    ])
    dfStopTimes = self.spark.read.csv(_objUri, header=True)
    dfStopTimes = dfStopTimes.rdd.map(lambda row: dict(
        trip_id=row.trip_id,
        stop_id=row.stop_id,
        time=row.arrival_time if row.arrival_time is not None else \
                                 row.departure_time,
        stop_seq=row.stop_sequence)
    )
    dfStopTimes = self.spark \
      .createDataFrame(dfStopTimes, schema=_schemaStopTimes)
    dfStopTimes = dfStopTimes \
      .withColumn("stop_seq", dfStopTimes.stop_seq.cast(IntegerType()))
    return dfStopTimes.filter("time is not NULL")


  def readStopTimes(self, feedDesc):
    prefix = '/'.join(["GTFS", feedDesc.s3Key])

    # rename some columns to disambiguate after joining the tables
    dfStops = self._fetch_stops_df(prefix) \
      .withColumnRenamed("stop_id", "stops_stop_id")
    dfStopTimes = self._fetch_stop_times_df(prefix)
    dfTrips = self._fetch_trips_df(prefix) \
      .withColumnRenamed("trip_id", "trips_trip_id")

    dfStopTimes = dfStopTimes \
      .join(dfStops, dfStopTimes.stop_id == dfStops.stops_stop_id)
    dfStopTimes = dfStopTimes \
      .join(dfTrips, dfStopTimes.trip_id == dfTrips.trips_trip_id)
    colNames = [
      "trip_id", "stop_id", "time", "stop_seq", "stop_name", "location_type",
      "stop_lat", "stop_lon", "route_id"
    ]
    return dfStopTimes.select(colNames)


  @staticmethod
  def readFeedDescs():
    objKey = '/'.join(["GTFS", "MBTA_archived_feeds.txt"])
    s3Mgr = s3.S3Mgr()
    content = s3Mgr.fetch_object_body(objKey)
    return gtfs.read_feed_descs(content)


PqDate = namedtuple("PqDate", "Date IsInVPDelays IsInHlyDelays")

def fetch_pqdates_to_update():
  ret = []
  sqlStmt = Queries["selectPqDatesWhere"] %\
    "NumRecs > 0 and (not IsInVPDelays or not IsInHlyDelays)"
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      ret.append(PqDate(row[0], row[1], row[2]))
  return ret


def delete_pqdate_from_table(D, tableName):
  sqlStmt = """
    DELETE FROM `%s` WHERE D = '%s';
  """ % (tableName, D.strftime("%Y-%m-%d"))
  with DBConn() as con:
    con.execute(sqlStmt)
    con.commit()

def set_pqdate_flag(D, flag):
  sqlStmt = """
    UPDATE `PqDates` SET `%s` = True
    WHERE D = '%s';
  """ % (flag, D.strftime("%Y-%m-%d"))
  with DBConn() as con:
    con.execute(sqlStmt)
    con.commit()


def read_vp_parquet(spark, targetDate):
  objUri = "VP-" + targetDate.strftime("%Y%m%d")
  objUri = "s3a://" + '/'.join((Settings.S3BucketName, "parquet", objUri))
  return spark.read.parquet(objUri)


def run(spark):
  with DBConnCommonQueries() as con:
    con.create_table("VPDelays", False)
    con.create_table("HlyDelays", False)

  feedDescs = GTFSFetcher.readFeedDescs()
  curFeedDesc = None
  dfStopTimes = None
  feedRequiredFiles = ["stops.txt", "stop_times.txt", "trips.txt"]

  gtfsFetcher = GTFSFetcher(spark)
  for entry in fetch_pqdates_to_update():
    targetDate = entry.Date

    if dfStopTimes is None or not curFeedDesc.includesDate(targetDate):
      curFeedDesc = None
      dfStopTimes = None
      for fd in feedDescs:
        if fd.includesDate(targetDate) and fd.includesFiles(feedRequiredFiles):
          curFeedDesc = fd
          dfStopTimes = gtfsFetcher.readStopTimes(curFeedDesc)
          print('USING FEED "%s" for %s' % \
                (curFeedDesc.version, targetDate.strftime("%Y-%m-%d")))
          break
    else:
      print('RE-USING FEED "%s" for %s' % \
            (curFeedDesc.version, targetDate.strftime("%Y-%m-%d")))

    if dfStopTimes:
      dfVehPos = read_vp_parquet(spark, targetDate)

      calcVPDelays = \
        VPDelaysCalculator(spark, targetDate, dfStopTimes, dfVehPos)
      dfVPDelays = calcVPDelays.createResultDF()

      if not entry.IsInVPDelays:
        delete_pqdate_from_table(targetDate, "VPDelays")
        calcVPDelays.updateDB(dfVPDelays)
        set_pqdate_flag(targetDate, "IsInVPDelays")

      calcHlyDelays = HlyDelaysCalculator(spark, dfVPDelays)
      dfHlyDelays = calcHlyDelays.createResultDF().persist()
      dfGrpRoutes = calcHlyDelays.groupRoutes(dfHlyDelays)
      dfGrpStops = calcHlyDelays.groupStops(dfHlyDelays)
      dfGrpAll = calcHlyDelays.groupAll(dfHlyDelays)
      dfHlyDelaysBus = dfHlyDelays.filter(dfHlyDelays.RouteId.rlike("^[0-9]"))
      dfHlyDelaysTrain = dfHlyDelays.filter(~dfHlyDelays.RouteId.rlike("^[0-9]"))
      dfGrpStopsBus = calcHlyDelays.groupStops(dfHlyDelaysBus)
      dfGrpAllBus = calcHlyDelays.groupAll(dfHlyDelaysBus)
      dfGrpStopsTrain = calcHlyDelays.groupStops(dfHlyDelaysTrain)
      dfGrpAllTrain = calcHlyDelays.groupAll(dfHlyDelaysTrain)

      if not entry.IsInHlyDelays:
        delete_pqdate_from_table(targetDate, "HlyDelays")
        calcHlyDelays.updateDB(dfHlyDelays, targetDate)
        calcHlyDelays.updateDB(dfGrpRoutes, targetDate)
        calcHlyDelays.updateDB(dfGrpStops, targetDate)
        calcHlyDelays.updateDB(dfGrpAll, targetDate)
        calcHlyDelays.updateDB(dfGrpStopsBus, targetDate, "ALLBUSES")
        calcHlyDelays.updateDB(dfGrpAllBus, targetDate, "ALLBUSES")
        calcHlyDelays.updateDB(dfGrpStopsTrain, targetDate, "ALLTRAINS")
        calcHlyDelays.updateDB(dfGrpAllTrain, targetDate, "ALLTRAINS")
        set_pqdate_flag(targetDate, "IsInHlyDelays")


if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue
  appName = "UpdateDelays"
  sparkSession = builder \
    .appName(appName) \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
