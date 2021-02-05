"""Module to combine GTFS schedule feeds with vehicle positions Parquet files
and update the VPDelays and HlyDelays tables
"""

# pylint: disable=cell-var-from-loop

import os
from datetime import date, datetime, timedelta
from collections import namedtuple

import pytz
import tzlocal
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DoubleType, IntegerType, \
  DateType, TimestampType, BooleanType
from third_party.transitfeed import shapelib
from common import Settings
from common import credentials, dbtables, gtfs, s3, utils
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def _compute_date_cutoffs(targetDate, retTZ=pytz.utc):
  # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
  lowerCutoff = datetime(targetDate.year, targetDate.month, targetDate.day,
                         8)
  dst_diff = abs(utils.dst_diff(targetDate))
  # If dst_diff is non-zero, the clock is moving backward or forward today
  # remove any values between 3 am and 4 am of the next day if the clock is
  # moving forward and between 2 am and 3 am if the clock is moving backward
  upperCutoff = datetime(targetDate.year, targetDate.month, targetDate.day,
                         8 - dst_diff) + timedelta(days=1)

  if retTZ != pytz.utc:
    lowerCutoff = pytz.utc.localize(lowerCutoff) \
      .astimezone(retTZ) \
      .replace(tzinfo=None)
    upperCutoff = pytz.utc.localize(upperCutoff) \
      .astimezone(retTZ) \
      .replace(tzinfo=None)

  return [lowerCutoff, upperCutoff]


class VPDelaysCalculator:
  """Class to handle calculations and updates to the VPDelays table
  """

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
    """Initializes the instance

    Args:
      spark: Spark Session object
      pqDate: a date of the Parquet file with vehicle positions
      dfStopTimes: dataframe of stop times from the schedule
      dfVehPos: dataframe of vehicle positions
    """

    self.spark = spark
    self.pqDate = pqDate
    self.dfStopTimes = dfStopTimes
    self.dfVehPos = dfVehPos


  def create_result_df(self):
    """Creates the delays dataframe from the schedule and vehicle positions
    dataframes
    """

    rddStopTimes = self.dfStopTimes.rdd \
      .map(lambda rec: (rec.trip_id, tuple(rec))) \
      .groupByKey()

    rddVehPos = self.dfVehPos.rdd \
      .map(lambda rec: (rec.TripId, tuple(rec))) \
      .groupByKey()

    pqDate = self.pqDate
    cutoffs = _compute_date_cutoffs(pqDate, pytz.utc)
    rddVPDelays = rddStopTimes.join(rddVehPos) \
      .flatMap(lambda keyTpl1Tpl2:
        VPDelaysCalculator._process_joined(pqDate, cutoffs, keyTpl1Tpl2))

    return self.spark.createDataFrame(rddVPDelays, self.TableSchema)


  def update_db(self, dfVPDelays):
    """Saves a delays dataframe to the database table VPDelays
    """
    pqDate = self.pqDate
    dfVPDelays.foreachPartition(lambda rows:
        VPDelaysCalculator._push_vpdelays_dbtpls(rows, pqDate)
    )


  @staticmethod
  def _process_joined(pqDate, cutoffs, keyTpl1Tpl2):
    stopTimeRecs = keyTpl1Tpl2[1][0]
    vehPosRecs = keyTpl1Tpl2[1][1]

    stopTimeLst = []
    for stopTimeRec in stopTimeRecs:
      try:
        dt = utils.sched_time_to_dt(stopTimeRec[2], pqDate)
        dt = Settings.MBTA_TZ.localize(dt).astimezone(pytz.UTC)
      except (pytz.exceptions.AmbiguousTimeError,
              pytz.exceptions.NonExistentTimeError):
        continue
      stopTimeLst.append(dict(
          tripId=stopTimeRec[0],
          stopId=stopTimeRec[1],
          stopSeq=stopTimeRec[3],
          stopName=stopTimeRec[4],
          routeId=stopTimeRec[8],
          coords=shapelib.Point.FromLatLng(stopTimeRec[6], stopTimeRec[7]),
          schedDT=dt.replace(tzinfo=None)
      ))

    vehPosLst = []
    for vehPosRec in vehPosRecs:
      vehPosLst.append(VPDelaysCalculator.VehPos(
          vehPosRec[3],
          datetime.utcfromtimestamp(vehPosRec[1].timestamp()),
          shapelib.Point.FromLatLng(vehPosRec[4], vehPosRec[5])
      ))

    return VPDelaysCalculator._calc_delays(cutoffs, stopTimeLst, vehPosLst)


  @staticmethod
  def _calc_delays(cutoffs, stopTimeLst, vehPosLst):
    ret = []

    for stopTime in stopTimeLst:
      stopCoords = stopTime["coords"]
      stopLatLon = stopCoords.ToLatLng()

      curClosest = None
      curDist = 1e10
      curSchedDT = None
      for vp in vehPosLst:
        dist = stopCoords.GetDistanceMeters(vp.coords)
        if dist < curDist:
          # adjust the scheduled time for possible date mismatches
          schedDT = stopTime["schedDT"]
          daysDiff = round((schedDT - vp.DT).total_seconds() / (24 * 3600))
          schedDT -= timedelta(days=daysDiff)
          # ignore datapoints where the absolute value of delay is too large
          if -2400 < (vp.DT - schedDT).total_seconds() < 2400:
            curDist = dist
            curClosest = vp
            curSchedDT = schedDT

      if curClosest and cutoffs[0] < schedDT < cutoffs[1]:
        vpLatLon = curClosest.coords.ToLatLng()
        ret.append((
            stopTime["routeId"], stopTime["tripId"], stopTime["stopId"],
            stopTime["stopName"], stopLatLon[0], stopLatLon[1], curSchedDT,
            vpLatLon[0], vpLatLon[1], curClosest.DT, curDist,
            (curClosest.DT - curSchedDT).total_seconds()
        ))
    return ret


  @staticmethod
  def _push_vpdelays_dbtpls(rows, pqDate):
    with DBConn() as conn:
      for row in rows:
        dbtables.VPDelays.insert_row(conn, row, pqDate)
        if conn.uncommited % 1000 == 0:
          conn.commit()
      conn.commit()


class HlyDelaysCalculator:
  """Class to handle calculations and updates to the HlyDelays table

  Aggregates data on delays by date and hour in the US/Eastern timezone
  """

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
    """Converts a UTC datetime to a date and hour in the US/Eastern time zone

    Args:
      dt: datetime to convert
    """

    dt = pytz.utc.localize(dt).astimezone(Settings.MBTA_TZ)
    return (dt.date(), dt.hour)


  def __init__(self, spark, dfVPDelays):
    """Initializes the instance

    Args:
      spark: Spark Session object
      dfVPDelays: dataframe containing unaggregated delays for trips and stops
    """

    self.spark = spark
    self.dfVPDelays = dfVPDelays


  def create_result_df(self):
    """Aggregates a delays dataframe so that it has data grouped by
    Date (US/Eastern time), Hour (US/Eastern time), RouteId, and StopName
    """

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


  def group_routes(self, dfHlyDelays):
    """Additionally aggregates an hourly delays dataframe so that it has data
    grouped by Date (US/Eastern time), Hour (US/Eastern time), and RouteId
    """

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


  def group_stops(self, dfHlyDelays):
    """Additionally aggregates an hourly delays dataframe so that it has data
    grouped by Date (US/Eastern time), Hour (US/Eastern time), and StopName
    """

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


  def group_all(self, dfHlyDelays):
    """Additionally aggregates an hourly delays dataframe so that it has data
    grouped by Date (US/Eastern time) and Hour (US/Eastern time) only
    """

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


  def update_db(self, dfHlyDelays, pqDate, noRouteVal=None):
    """Saves a delays dataframe to the database table HlyDelays
    """

    dfHlyDelays.foreachPartition(lambda rows:
        HlyDelaysCalculator._push_hlydelays_dbtpls(rows, pqDate, noRouteVal)
    )


  @staticmethod
  def _push_hlydelays_dbtpls(rows, pqDate, noRouteVal):
    with DBConn() as conn:
      for row in rows:
        dbtables.HlyDelays.insert_row(conn, row, pqDate, noRouteVal)
        if conn.uncommited % 1000 == 0:
          conn.commit()
      conn.commit()


class GTFSFetcher:
  """Class to create stop times dataframe from GTFS schedule on S3
  """

  def __init__(self, spark):
    """Initializes the instance

    Args:
      spark: Spark Session object
    """

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


  def read_stop_times(self, feedDesc):
    """Reads a GTFS feed and returns a stop times dataframe

    Args:
      feedDesc: an MBTA_ArchivedFeedDesc object
    """

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
  def read_feed_descs():
    """Retrieves a list of GTFS feed descriptions available on S3
    """

    objKey = '/'.join(["GTFS", "MBTA_archived_feeds.txt"])
    s3Mgr = s3.S3Mgr()
    content = s3Mgr.fetch_object_body(objKey)
    return gtfs.read_feed_descs(content)


def read_vp_parquet(spark, targetDate):
  """Creates a dataframe for a Parquet file

  Args:
    spark: Spark Session object
    targetDate: date for which to read the Parquet
  """

  objUri = "VP-" + targetDate.strftime("%Y%m%d")
  objUri = "s3a://" + '/'.join((Settings.S3BucketName, "parquet", objUri))
  ret = spark.read.parquet(objUri)

  cutoffs = _compute_date_cutoffs(targetDate, tzlocal.get_localzone())
  udf_filter_for_pqdate = F.udf(
    lambda dt: cutoffs[0] < dt < cutoffs[1],
    BooleanType()
  )
  ret = ret.filter(udf_filter_for_pqdate(ret.DT))
  return ret


def run(spark):
  """Combines GTFS schedule feed with vehicle positions Parquet files
  and updates the VPDelays and HlyDelays tables

  Args:
    spark: Spark Session object
  """

  log = utils.get_logger()

  with DBConnCommonQueries() as conn:
    dbtables.create_if_not_exists(conn, dbtables.VPDelays)
    dbtables.create_if_not_exists(conn, dbtables.HlyDelays)

  feedDescs = GTFSFetcher.read_feed_descs()
  curFeedDesc = None
  dfStopTimes = None
  feedRequiredFiles = ["stops.txt", "stop_times.txt", "trips.txt"]

  gtfsFetcher = GTFSFetcher(spark)
  # with DBConn() as conn:
  #   entriesToProcess = dbtables.PqDates \
  #     .select_pqdates_not_in_delays(conn, 'NOT IsInHlyDelays')
  entriesToProcess = [date(2020, 8, 20)]
  for targetDate in entriesToProcess:
    if dfStopTimes is None or not curFeedDesc.includes_date(targetDate):
      curFeedDesc = None
      dfStopTimes = None
      for fd in feedDescs:
        if fd.includes_date(targetDate) and fd.includes_files(feedRequiredFiles):
          curFeedDesc = fd
          dfStopTimes = gtfsFetcher.read_stop_times(curFeedDesc)
          log.info('USING FEED "%s" for %s', curFeedDesc.version,
                   targetDate.strftime("%Y-%m-%d"))
          break
    else:
      log.info('RE-USING FEED "%s" for %s', curFeedDesc.version,
               targetDate.strftime("%Y-%m-%d"))

    if dfStopTimes:
      dfVehPos = read_vp_parquet(spark, targetDate)

      calcVPDelays = \
        VPDelaysCalculator(spark, targetDate, dfStopTimes, dfVehPos)
      dfVPDelays = calcVPDelays.create_result_df()

      with DBConn() as conn:
        dbtables.VPDelays.delete_for_parquet(conn, targetDate)
        conn.commit()
      calcVPDelays.update_db(dfVPDelays)

      calcHlyDelays = HlyDelaysCalculator(spark, dfVPDelays)
      dfHlyDelays = calcHlyDelays.create_result_df().persist()
      dfGrpRoutes = calcHlyDelays.group_routes(dfHlyDelays)
      dfGrpStops = calcHlyDelays.group_stops(dfHlyDelays)
      dfGrpAll = calcHlyDelays.group_all(dfHlyDelays)
      dfHlyDelaysBus = dfHlyDelays.filter(dfHlyDelays.RouteId.rlike("^[0-9]"))
      dfHlyDelaysTrain = dfHlyDelays.filter(~dfHlyDelays.RouteId.rlike("^[0-9]"))
      dfGrpStopsBus = calcHlyDelays.group_stops(dfHlyDelaysBus)
      dfGrpAllBus = calcHlyDelays.group_all(dfHlyDelaysBus)
      dfGrpStopsTrain = calcHlyDelays.group_stops(dfHlyDelaysTrain)
      dfGrpAllTrain = calcHlyDelays.group_all(dfHlyDelaysTrain)

      with DBConn() as conn:
        dbtables.HlyDelays.delete_for_parquet(conn, targetDate)
        conn.commit()

      calcHlyDelays.update_db(dfHlyDelays, targetDate)
      calcHlyDelays.update_db(dfGrpRoutes, targetDate)
      calcHlyDelays.update_db(dfGrpStops, targetDate)
      calcHlyDelays.update_db(dfGrpAll, targetDate)
      calcHlyDelays.update_db(dfGrpStopsBus, targetDate, "ALLBUSES")
      calcHlyDelays.update_db(dfGrpAllBus, targetDate, "ALLBUSES")
      calcHlyDelays.update_db(dfGrpStopsTrain, targetDate, "ALLTRAINS")
      calcHlyDelays.update_db(dfGrpAllTrain, targetDate, "ALLTRAINS")


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
