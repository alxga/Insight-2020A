# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types \
  import StringType, TimestampType, DoubleType, IntegerType

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
      # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
      if dtUtcNow > datetime(dt.year, dt.month, dt.day + 1, 8):
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
    # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
    dt1 = datetime(dt.year, dt.month, dt.day, 8)
    dt2 = datetime(dt.year, dt.month, dt.day + 1, 8)
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
      eachVehiclePos=lambda x: ret.append(gtfsrt.vehpos_pb2_to_dbtpl_dtlocal(x))
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
    print("Got %d keys of %s" % (len(keys), str(targetDate)), flush=True)
    if len(keys) <= 0:
      continue

    rddVP = spark.sparkContext \
      .parallelize(keys) \
      .flatMap(fetch_tpls) \
      .map(lambda tpl: ((tpl[1], tpl[3]), tpl)) \
      .reduceByKey(lambda x, y: x).map(lambda x: x[1])

    print("Reduced by key %d keys of %s" % (len(keys), str(targetDate)), flush=True)

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
    dfVP = dfVP.orderBy("DT", ascending=True)
    print("Sorted by DT %d keys of %s" % (len(keys), str(targetDate)), flush=True)

    pqKey = targetDate.strftime("%Y%m%d")
    pqKey = '/'.join(["parquet", "VP-" + pqKey])
    pqKey = "s3a://alxga-insde/%s" % pqKey
    dfVP.write.format("parquet").mode("overwrite").save(pqKey)
    print("Written to Parquet %d keys of %s" % (len(keys), str(targetDate)), flush=True)

    print("Updating the VehPosPb table %s" % pqKey)
    spark.sparkContext \
      .parallelize(keys) \
      .foreachPartition(lambda x: set_vehpospb_flag("IsInPq", "TRUE", x))
    print("Updated IsInPq for %d keys of %s" % (len(keys), str(targetDate)), flush=True)

  spark.stop()
