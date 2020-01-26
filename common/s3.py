from datetime import datetime
import boto3
from .settings import Settings
from .credentials import S3ConnArgs

class S3Mgr:
  def __init__(self, bucketName=Settings.S3BucketName):
    self._Res = boto3.resource('s3', **S3ConnArgs)
    self.bucketName = bucketName

  def move_key(self, objKey, nObjKey):
    self._Res.Object(self.bucketName, nObjKey). \
      copy_from(
        CopySource={
          'Bucket': self.bucketName, 'Key': objKey
        }
      )
    self._Res.Object(self.bucketName, objKey).delete()

  def fetch_keys(self, prefix, limit=None):
    objKeys = []
    bucket = self._Res.Bucket(self.bucketName)
    for obj in bucket.objects.filter(Prefix=prefix):
      objKeys.append(obj.key)
      if limit and len(objKeys) > limit:
        break
    return objKeys

  def fetch_object_body(self, objKey):
    obj = self._Res.Object(self.bucketName, objKey)
    body = obj.get()["Body"].read()
    return body

  def upload_file(self, fPath, objKey):
    self._Res.Object(self.bucketName, objKey).upload_file(fPath)


def S3FeedKeyDT(objKey):
  dtval = objKey[-18:-3] # Naming assumed: 'pb/<Feed Name>/YYYYMMDD/HHMMSS.pb2'
  dt = datetime.strptime(dtval, "%Y%m%d/%H%M%S")
  return dt
