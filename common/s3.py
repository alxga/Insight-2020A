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
