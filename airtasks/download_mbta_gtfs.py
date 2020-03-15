"""Script to download MBTA schedule if it's updated"""

import zipfile

from io import BytesIO
import requests

from common import s3, gtfs

__author__ = "Alex Ganin"


def update_archive_txt():
  """Retrieves a text file describing the currently available archives from
  MBTA and stores it in S3 if it can be successfully parsed by the app
  """

  url = "https://cdn.mbta.com/archive/archived_feeds.txt"
  r = requests.get(url)

  # parse first to check if we can work with this
  feedDescs = gtfs.read_feed_descs(r.content)

  objKey = '/'.join(["GTFS", "MBTA_archived_feeds.txt"])
  s3Mgr = s3.S3Mgr()
  s3Mgr.put_object_body(objKey, r.content)

  return feedDescs


def upload_zip(s3Mgr, feedDesc, s3Key):
  """Retrieves a GTFS archive for the feed description, extracts and stores
  it in S3

  Args:
    s3Mgr: an S3Mgr object to use for S3 operations
    feedDesc: a MBTA_ArchivedFeedDesc object describing the feed
    s3Key: a prefix to use for uploading files to S3
  """

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
  """Checks whether all feed descriptions are available in S3, then
  retrieves and downloads the missing ones

  Args:
    feedDescs: a list of MBTA_ArchivedFeedDesc objects
  """

  s3Mgr = s3.S3Mgr()
  for feedDesc in feedDescs:
    s3Key = '/'.join(["GTFS", feedDesc.s3Key])
    archS3Key = '/'.join(["GTFS_Archived", feedDesc.s3Key])
    if not s3Mgr.prefix_exists(s3Key) and not s3Mgr.prefix_exists(archS3Key):
      upload_zip(s3Mgr, feedDesc, s3Key)


def main():
  """Checks for feed updates on the MBTA website and saves any updates to S3
  """

  feedDescs = update_archive_txt()
  upload_zips(feedDescs)

if __name__ == "__main__":
  main()
