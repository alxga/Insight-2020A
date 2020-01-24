# pylint: disable=unused-import
from operator import add
from datetime import datetime, timedelta

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3
from google.protobuf.message import DecodeError
import gtfs_realtime_pb2

_s3Bucket = "alxga-insde"
_s3Res = boto3.resource('s3')


def fetch_keys():
  objKeys = []
  bucket = _s3Res.Bucket(_s3Bucket)
  for obj in bucket.objects.filter(Prefix='pb/VehiclePos'):
    objKeys.append(obj.key)
    if len(objKeys) > 100:
      break
  return objKeys

def pb2db_vehicle_pos(pbVal):
  tStamp = datetime.fromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  return (
    pbVal.trip.route_id, tStamp.strftime('%Y-%m-%d %H:%M:%S'),
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

def fetch_tpls(objKey):
  message = gtfs_realtime_pb2.FeedMessage()
  obj = _s3Res.Object(_s3Bucket, objKey)
  body = obj.get()["Body"].read()
  try:
    message.ParseFromString(body)
  except DecodeError:
    return

  tpls = []
  for entity in message.entity:
    # if entity.HasField('alert'):
    #   process_alert(entity.alert)
    # if entity.HasField('trip_update'):
    #   process_trip_update(entity.trip_update)
    if entity.HasField('vehicle'):
      tpls.append(pb2db_vehicle_pos(entity.vehicle))


if __name__ == "__main__":
  spark = SparkSession\
      .builder\
      .appName("PythonTestScript")\
      .getOrCreate()

  keys = fetch_keys()
  file_list = spark.sparkContext.parallelize(keys)
  counts = file_list.rdd \
    .flatMap(fetch_tpls) \
    .map(lambda x: (x[0], 1)) \
    .reduceByKey(add)

  output = counts.collect()
  for (word, count) in output:
    print("%s: %i" % (word, count))

  spark.stop()
