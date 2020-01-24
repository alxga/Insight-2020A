# pylint: disable=unused-import

import os
from operator import add
from datetime import datetime, timedelta

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3
from google.protobuf.message import DecodeError
import gtfs_realtime_pb2


_s3ConnArgs = {
  "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
  "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
  "region_name": os.environ["AWS_DEFAULT_REGION"]
}

def fetch_keys():
  s3Bucket = "alxga-insde"
  s3Res = boto3.resource('s3')

  objKeys = []
  bucket = s3Res.Bucket(s3Bucket)
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

def fetch_tpls(objKey, s3ConnArgs):
  message = gtfs_realtime_pb2.FeedMessage()

  s3Bucket = "alxga-insde"
  s3Res = boto3.resource('s3', **s3ConnArgs)

  obj = s3Res.Object(s3Bucket, objKey)
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
  return tpls


if __name__ == "__main__":
  spark = SparkSession\
      .builder\
      .appName("PythonTestScript")\
      .getOrCreate()

  keys = fetch_keys()
  file_list = spark.sparkContext.parallelize(keys)
  counts = file_list \
    .flatMap(lambda x: fetch_tpls(x, _s3ConnArgs)) \
    .map(lambda x: (x[1], 1)) \
    .reduceByKey(add)

  output = counts.collect()
  for (word, count) in output:
    print("%s: %i" % (word, count))

  spark.stop()
