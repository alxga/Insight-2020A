"""Module to update the vehicle positions database table
"""

import os

from pyspark.sql import SparkSession

from common import credentials, dbtables
from common.queryutils import DBConn, DBConnCommonQueries

__author__ = "Alex Ganin"


def push_vehpos_db(keyTpls):
  """Adds multiple records to the VehPos table

  Args:
    keyTpls: a tuple of the form (key, tpls) where key is unused and tpls
      are inserted into the table
  """

  with DBConn() as conn:
    tpls = []
    for keyTpl in keyTpls:
      tpls.append(keyTpl[1])
      if len(tpls) >= 100:
        dbtables.VehPos.insertTpls(conn, tpls)
        conn.commit()
        tpls = []
    if len(tpls) > 0:
      dbtables.VehPos.insertTpls(conn, tpls)
      conn.commit()


def set_vehpospb_invehpos(objKeys):
  """Marks S3 Protobuf keys as processed into the VehPos table

  Args:
    objKeys: keys for the Protobuf S3 objects
  """

  with DBConn() as conn:
    for objKey in objKeys:
      dbtables.VehPosPb.updateInVehPos(conn, objKey)
      if conn.uncommited >= 100:
        conn.commit()
    conn.commit()


def run(spark):
  """Updates the vehicle positions database table

  Args:
    spark: Spark Session object
  """

  with DBConnCommonQueries() as conn:
    dbtables.create_if_not_exists(conn, dbtables.VehPos)

  with DBConn() as conn:
    keys = dbtables.VehPosPb.selectProtobufKeysNotInVehPos(conn)
  print("Got %d keys" % len(keys), flush=True)

  step = 1000
  for i in range(0, len(keys), step):
    lower = i
    upper = i + step if i + step < len(keys) else len(keys)
    keysSubrange = keys[lower:upper]
    records = spark.sparkContext \
      .parallelize(keysSubrange) \
      .flatMap(dbtables.VehPos.buildDBTuplesFromProtobuf) \
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
