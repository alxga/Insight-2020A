"""Helpers to work with S3"""

from datetime import datetime
import boto3
from .settings import Settings
from .credentials import S3ConnArgs

__author__ = "Alex Ganin"


class S3Mgr:
  """Wrapper around boto3 resource and client interfaces"""

  def __init__(self, bucketName=Settings.S3BucketName):
    """Initializes the instance

    Args:
      bucketName: optional name of the bucket,
        defaults to Settings.S3BucketName
    """

    self._Res = boto3.resource('s3', **S3ConnArgs)
    self._Client = boto3.client('s3', **S3ConnArgs)
    self.bucketName = bucketName


  def move_key(self, objKey, nObjKey):
    """Renames/Moves a key

    Implemented as a copy and delete sequence of operations

    Args:
      objKey: key to rename or move
      nObjKey: new key under which to save the object
    """

    self._Res.Object(self.bucketName, nObjKey). \
      copy_from(
        CopySource={
          'Bucket': self.bucketName, 'Key': objKey
        }
      )
    self._Res.Object(self.bucketName, objKey).delete()


  def fetch_keys(self, prefix, limit=None):
    """Retrieves a list of keys under a given prefix from S3

    Args:
      prefix: prefix to search the keys
      limit: optional upper limit on the number of returned values
    """

    objKeys = []
    bucket = self._Res.Bucket(self.bucketName)
    for obj in bucket.objects.filter(Prefix=prefix):
      if limit and len(objKeys) >= limit:
        break
      objKeys.append(obj.key)
    return objKeys


  def fetch_object_body(self, objKey):
    """Retrieves an S3 object contents from S3

    Args:
      objKey: key of the object whose contents to retrieve
    """

    obj = self._Res.Object(self.bucketName, objKey)
    body = obj.get()["Body"].read()
    return body


  def put_object_body(self, objKey, data):
    """Writes an S3 object contents to S3

    Args:
      objKey: key of the object whose contents to write
    """
    obj = self._Res.Object(self.bucketName, objKey)
    obj.put(Body=data)


  def upload_file(self, fPath, objKey):
    """Uploads a local file to S3

    Args:
      fPath: path of a local file to upload
      objKey: key of the object for the file contents
    """
    self._Res.Object(self.bucketName, objKey).upload_file(fPath)


  def prefix_exists(self, prefix):
    """Checks whether a prefix exists in S3

    Args:
      prefix: prefix to check
    """
    result = self._Client.list_objects_v2(
        Bucket=self.bucketName, MaxKeys=1, Prefix=prefix
    )
    return result["KeyCount"] > 0


def S3FeedKeyDT(objKey):
  """Extracts a datetime from an S3 Protobuf file key
  """
  dtval = objKey[-18:-3] # Naming assumed: 'pb/<Feed Name>/YYYYMMDD/HHMMSS.pb2'
  dt = datetime.strptime(dtval, "%Y%m%d/%H%M%S")
  return dt
