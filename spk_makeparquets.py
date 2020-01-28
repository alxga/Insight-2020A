# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta
import pytz

import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, DateType, DoubleType, IntegerType

from common import credentials
from common import Settings, s3, utils, gtfsrt
from common.queries import Queries
from common.queryutils import set_vehpospb_flag

__author__ = "Alex Ganin"


def fetch_dates_to_update():
  cnx = None
  cursor = None
  sqlStmt = """
    SELECT DISTINCT Date(S3KeyDT) FROM VehPosPb WHERE not IsInPq;
  """
  dtUtcNow = datetime.utcnow()

  ret = []
  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    cursor.execute(sqlStmt)
    for tpl in cursor:
      dt = tpl[0]
      # we're defining new day to start at 3 in the night
      if dtUtcNow > datetime(dt.year, dt.month, dt.day + 1, 3):
        ret.append(dt)
    return ret
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()

def fetch_keys_to_update(dt):
  cnx = None
  cursor = None
  sqlStmt = Queries["selectVehPosPb_forDate"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    # we're defining new day to start at 3 in the night
    dt1 = datetime(dt.year, dt.month, dt.day, 3).astimezone(pytz.UTC)
    dt2 = datetime(dt.year, dt.month, dt.day + 1, 3).astimezone(pytz.UTC)
    cursor.execute(sqlStmt, (dt1, dt2))

    ret = []
    for tpl in cursor:
      ret.append(tpl[0])
    return ret
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()


def fetch_tpls(objKey):
  ret = []
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  gtfsrt.process_entities(data,
      eachVehiclePos=lambda x: ret.append(gtfsrt.vehpos_pb2_to_dbtpl(x))
  )
  return ret

if __name__ == "__main__":
  builder = SparkSession.builder
  for envVar in credentials.EnvVars:
    try:
      val = os.environ[envVar]
      confKey = "spark.executorEnv.%s" % envVar
      builder = builder.config(confKey, val)
    except KeyError:
      continue
  spark = builder.appName("UpdateVehPos") \
                 .getOrCreate()


  targetDates = fetch_dates_to_update()
  for targetDate in targetDates:
    keys = fetch_keys_to_update(targetDate)
    print("Got %d keys to deal with for %s" % (len(keys), str(targetDate)))


    rddVP = spark.sparkContext \
      .parallelize(keys) \
      .flatMap(fetch_tpls) \
      .map(lambda tpl: ((tpl[1], tpl[3]), tpl)) \
      .reduceByKey(lambda x, y: x).map(lambda x: x[1])

    schema = StructType([
      StructField("RouteId", StringType(), True),
      StructField("DT", DateType(), False),
      StructField("VehicleId", StringType(), False),
      StructField("TripId", StringType(), False),
      StructField("Lat", DoubleType(), False),
      StructField("Lon", DoubleType(), False),
      StructField("Status", IntegerType(), True),
      StructField("StopSeq", IntegerType(), True),
      StructField("StopId", StringType(), True),
    ])
    dfVP = spark.createDataFrame(rddVP, schema)
    dfVP = dfVP.orderBy("DT", ascending=True)

    pqKey = targetDate.strftime("%Y%m%d")
    pqKey = '/'.join(["parquet", "VP-" + pqKey])
    pqKey = "s3a://alxga-insde/%s" % pqKey
    dfVP.write.format("parquet").mode("overwrite").save(pqKey)

    print("Updating the VehPosPb table %s" % pqKey)
    spark.sparkContext \
      .parallelize(keys) \
      .foreachPartition(lambda x: set_vehpospb_flag("IsInPq", "TRUE", x))

  spark.stop()
