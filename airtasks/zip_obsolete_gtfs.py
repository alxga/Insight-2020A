"""Script to zip MBTA schedule in S3 to save space"""

import tarfile

from io import BytesIO

from common.queryutils import DBConn
from common import Settings, s3, gtfs, dbtables

__author__ = "Alex Ganin"


def archive_gtfs_files(s3Mgr, feedDesc):
  """ Compresses text files from a feed in S3, uploads the archive to S3,
  and removes the text files

  Uses tar.bz2 format

  Args:
    s3Mgr: an S3Mgr object to use for S3 operations
    feedDesc: a MBTA_ArchivedFeedDesc object describing the feed
  """
  pfx = '/'.join(["GTFS", feedDesc.s3Key]) + '/'
  objKeys = s3Mgr.fetch_keys(pfx)
  if not objKeys:
    return False
  buffer = BytesIO()
  tbz2 = tarfile.open(mode="w:bz2", fileobj=buffer)
  for objKey in objKeys:
    if not objKey[-4:] == '.txt':
      continue
    data = s3Mgr.fetch_object_body(objKey)
    tarInfo = tarfile.TarInfo(objKey.split('/')[-1])
    tarInfo.size = len(data)
    tbz2.addfile(tarinfo=tarInfo, fileobj=BytesIO(data))
  tbz2.close()
  objKey = '/'.join(["GTFS_Archived", feedDesc.s3Key]) + ".tar.bz2"
  s3Mgr.put_object_body(objKey, buffer.getvalue())

  for objKey in objKeys:
    s3Mgr.delete_key(objKey)


def main():
  """Compresses text files from obsolete feeds in S3, uploads archives to S3,
  and removes the text files

  Uses tar.bz2 format
  """
  s3Mgr = s3.S3Mgr()
  objKey = '/'.join(["GTFS", "MBTA_archived_feeds.txt"])
  content = s3Mgr.fetch_object_body(objKey)
  feedDescs = gtfs.read_feed_descs(content)

  with DBConn() as conn:
    dtNow = dbtables.PqDates.select_latest_processed(conn)
  if not dtNow:
    return

  for fd in feedDescs:
    daysDiff = (dtNow - fd.endDate).total_seconds() / (24 * 3600)
    if daysDiff > Settings.GTFS_ObsoleteAfterDays:
      archive_gtfs_files(s3Mgr, fd)


if __name__ == "__main__":
  main()
