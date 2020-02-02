# pylint: disable=unused-variable
# pylint: disable=cell-var-from-loop

import os
from datetime import date, datetime, timedelta
from collections import namedtuple

import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DoubleType, IntegerType

from transitfeed import shapelib
from common import credentials
from common import Settings, s3, utils, gtfs
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"

_Test_Perf = True
_Test_Perf_Parquet = False
_Test_Perf_DB = True


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


def count_dbtpls(tpls):
  count = 0
  for tpl in tpls:
    count += 1
  print("Got %d tuples in some partition for testing" % count)

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

  stopTimesRDD = stopTimesDF.rdd \
    .map(lambda rec: (rec.trip_id, tuple(rec))) \
    .groupByKey()

  return stopTimesRDD


def test_perf_db(spark, targetDate, stopTimesRDD):
  connArgs = credentials.MySQLConnArgs

  db_properties = {}
  db_url = "jdbc:mysql://%s:3306/%s" % (connArgs["host"], connArgs["database"])
  db_properties['user'] = connArgs["user"]
  db_properties['password'] = connArgs["password"]
  db_properties['url'] = db_url
  db_properties['driver'] = "com.mysql.cj.jdbc.Driver"

  step_td = timedelta(minutes=60)
  num_steps = int((24 * 3600) / step_td.total_seconds())
  # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
  dt = datetime(targetDate.year, targetDate.month, targetDate.day, 8)
  for step in range(0, num_steps):
    sqlStmt = Queries["selectVehPos_forDate"] % (
        dt.strftime('%Y-%m-%d %H:%M'),
        (dt + step_td).strftime('%Y-%m-%d %H:%M')
    )
    sqlStmt = "(%s) as VP" % sqlStmt.replace(';', '')

    vpDF = spark.read.jdbc(
        url=db_url, table=sqlStmt, properties=db_properties
    )
    vpRDD = vpDF.rdd \
      .map(lambda rec: (rec.TripId, tuple(rec))) \
      .groupByKey()
    joinRDD = stopTimesRDD.join(vpRDD) \
      .flatMap(lambda x: process_joined(targetDate, x)) \
      .foreachPartition(count_dbtpls)

    dt += step_td


def read_vp_parquet(spark, targetDate):
  objUri = "VP-" + targetDate.strftime("%Y%m%d")
  objUri = "s3a://" + '/'.join((Settings.S3BucketName, "parquet", objUri))
  vpDF = spark.read.parquet(objUri)
  return vpDF.rdd \
    .map(lambda rec: (rec.TripId, tuple(rec)))

def test_perf_parquet(spark, targetDate, stopTimesRDD):
  vpRDD = read_vp_parquet(spark, targetDate) \
    .groupByKey()
  joinRDD = stopTimesRDD.join(vpRDD) \
    .flatMap(lambda x: process_joined(targetDate, x)) \
    .foreachPartition(count_dbtpls)


def run(spark):
  if not _Test_Perf:
    with DBConnCommonQueries() as con:
      con.create_table("VPDelays", False)

  feedDescs = fetch_feed_descs()
  curFeedDesc = None
  stopTimesRDD = None
  feedRequiredFiles = ["stops.txt", "stop_times.txt", "trips.txt"]

  targetDates = fetch_pqdts_to_update() if not _Test_Perf \
    else [date(2020, 1, 20)]

  for targetDate in targetDates:

    if stopTimesRDD is None or not curFeedDesc.includesDate(targetDate):
      curFeedDesc = None
      stopTimesRDD = None
      for fd in feedDescs:
        if fd.includesDate(targetDate) and fd.includesFiles(feedRequiredFiles):
          curFeedDesc = fd
          stopTimesRDD = read_joint_stop_times_df(spark, curFeedDesc)
          print('USING FEED "%s" for %s' % \
                (curFeedDesc.version, targetDate.strftime("%Y-%m-%d")))
          break
    else:
      print('RE-USING FEED "%s" for %s' % \
            (curFeedDesc.version, targetDate.strftime("%Y-%m-%d")))

    if stopTimesRDD:
      if _Test_Perf:
        if _Test_Perf_Parquet:
          test_perf_parquet(spark, targetDate, stopTimesRDD)
        elif _Test_Perf_DB:
          test_perf_db(spark, targetDate, stopTimesRDD)
      else:
        vpRDD = read_vp_parquet(spark, targetDate) \
          .groupByKey()
        joinRDD = stopTimesRDD.join(vpRDD) \
          .flatMap(lambda x: process_joined(targetDate, x)) \
          .foreachPartition(push_vpdelays_dbtpls)

    if not _Test_Perf:
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
  appName = "CalcVPDelays"
  if _Test_Perf:
    appName += "_Test_Perf"
    if _Test_Perf_Parquet:
      appName += "_Parquet"
    elif _Test_Perf_DB:
      appName += "_DB"
  sparkSession = builder \
    .appName(appName) \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
