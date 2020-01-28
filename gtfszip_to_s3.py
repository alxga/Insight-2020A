# pylint: disable=unused-import

import os
import argparse
import zipfile
from common import s3, Settings

__author__ = "Alex Ganin"


s3Mgr = s3.S3Mgr()

def upload_gtfszip(zipPath):
  s3Pfx = os.path.basename(zipPath)
  s3Pfx = os.path.splitext(s3Pfx)[0]
  s3Pfx = '/'.join(["GTFS", s3Pfx])

  z = zipfile.ZipFile(zipPath, mode="r")

  try:
    for name in z.namelist():
      fContent = z.read(name)
      s3Key = '/'.join([s3Pfx, name])
      s3Mgr.put_object_body(s3Key, fContent)
  finally:
    z.close()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Upload static GTFS archive to S3')
  parser.add_argument('zip_path', metavar='ZipPath', type=str, nargs=1,
                      help='Path to the archive containing the feed')

  args = parser.parse_args()
  upload_gtfszip(args.zip_path[0])
