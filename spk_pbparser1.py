# pylint: disable=unused-import
from operator import add

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3
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



if __name__ == "__main__":
  spark = SparkSession\
      .builder\
      .appName("PythonTestScript")\
      .getOrCreate()

  keys = fetch_keys()
  file_list = spark.sparkContext.parallelize(keys)
  
  lines = lines.rdd.map(lambda r: r[0])
  counts = lines.flatMap(lambda x: x.split(' ')) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(add)
  output = counts.collect()
  for (word, count) in output:
    print("%s: %i" % (word, count))

  spark.stop()
