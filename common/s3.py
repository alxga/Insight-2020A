from datetime import datetime
import boto3
from .settings import Settings
from .credentials import S3ConnArgs

def fetch_keys(prefix):
  s3Bucket = Settings.S3BucketName
  s3Res = boto3.resource('s3', **S3ConnArgs)

  objKeys = []
  bucket = s3Res.Bucket(s3Bucket)
  for obj in bucket.objects.filter(Prefix=prefix):
    objKeys.append(obj.key)
    if len(objKeys) > 100:
      break
  return objKeys


def fetch_object_body(objKey):
  s3Bucket = Settings.S3BucketName
  s3Res = boto3.resource('s3', **S3ConnArgs)

  obj = s3Res.Object(s3Bucket, objKey)
  body = obj.get()["Body"].read()
  return body


def S3FeedKeyDT(objKey):
  dtval = objKey[-18:-3] # Naming assumed: 'pb/<Feed Name>/YYYYMMDD-HHMMSS.pb2'
  dt = datetime.strptime(dtval, "%Y%m%d-%H%M%S")
  return dt
