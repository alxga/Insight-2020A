import zipfile

from io import BytesIO
import requests

from common import s3, gtfs

__author__ = "Alex Ganin"


def update_archive_txt():
  url = "https://cdn.mbta.com/archive/archived_feeds.txt"
  r = requests.get(url)

  # parse first to check if we can work with this
  feedDescs = gtfs.read_feed_descs(r.content)

  objKey = '/'.join(["GTFS", "MBTA_archived_feeds.txt"])
  s3Mgr = s3.S3Mgr()
  s3Mgr.put_object_body(objKey, r.content)

  return feedDescs


def upload_zip(s3Mgr, feedDesc, s3Key):
  r = requests.get(feedDesc.url)
  contentFile = BytesIO(r.content)
  z = zipfile.ZipFile(contentFile)
  try:
    for name in z.namelist():
      fContent = z.read(name)
      s3ObjKey = '/'.join([s3Key, name])
      s3Mgr.put_object_body(s3ObjKey, fContent)
  finally:
    z.close()


def upload_zips(feedDescs):
  s3Mgr = s3.S3Mgr()
  for feedDesc in feedDescs:
    s3Key = '/'.join(["GTFS", feedDesc.s3Key])
    if not s3Mgr.prefix_exists(s3Key):
      upload_zip(s3Mgr, feedDesc, s3Key)


def main():
  feedDescs = update_archive_txt()
  upload_zips(feedDescs)

if __name__ == "__main__":
  main()
