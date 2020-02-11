import os

from pyspark.sql import SparkSession

from common import credentials, s3, gtfsrt
from common.queries import Queries
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def fetch_keys_to_update():
  sqlStmt = """
    SELECT S3Key FROM `VehPosPb`
    WHERE NumRecs > 0 and not IsInVehPos;
  """
  ret = []
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for tpl in cur:
      ret.append(tpl[0])
  return ret


def fetch_tpls(objKey):
  ret = []
  s3Mgr = s3.S3Mgr()
  data = s3Mgr.fetch_object_body(objKey)
  gtfsrt.process_entities(data,
      eachVehiclePos=lambda x: ret.append(gtfsrt.vehpos_pb2_to_dbtpl_dtutc(x))
  )
  return ret


def push_vehpos_db(keyTpls):
  sqlStmt = Queries["insertVehPos"]
  with DBConn() as con:
    tpls = []
    for keyTpl in keyTpls:
      tpls.append(keyTpl[1])
      if len(tpls) >= 100:
        con.executemany(sqlStmt, tpls)
        con.commit()
        tpls = []
    if len(tpls) > 0:
      con.executemany(sqlStmt, tpls)
      con.commit()


def set_vehpospb_invehpos(objKeys):
  sqlStmtMsk = """
    UPDATE `VehPosPb` SET `IsInVehPos` = True
    WHERE S3Key = '%s';
  """
  with DBConnCommonQueries() as con:
    for objKey in objKeys:
      con.execute(sqlStmtMsk % objKey)
      if con.uncommited >= 100:
        con.commit()
    con.commit()


def run(spark):
  keys = fetch_keys_to_update()
  print("Got %d keys" % len(keys), flush=True)

  step = 1000
  for i in range(0, len(keys), step):
    lower = i
    upper = i + step if i + step < len(keys) else len(keys)
    keysSubrange = keys[lower:upper]
    records = spark.sparkContext \
      .parallelize(keysSubrange) \
      .flatMap(fetch_tpls) \
      .map(lambda tpl: ((tpl[1], tpl[3]), tpl)) \
      .reduceByKey(lambda x, y: x)

    records.foreachPartition(push_vehpos_db)
    print("Inserted records for keys  %d-%d" %
          (lower, upper - 1), flush=True)

    spark.sparkContext \
      .parallelize(keysSubrange) \
      .foreachPartition(set_vehpospb_invehpos)
    print("Updated IsInVehPos for keys %d-%d" %
          (lower, upper - 1), flush=True)


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
    .appName("UpdateVehPos") \
    .getOrCreate()

  run(sparkSession)

  sparkSession.stop()
