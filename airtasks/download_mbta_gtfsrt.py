"""Script to download MBTA vehicle positions every 5 seconds
during 1 minute
"""

import os
import time
import logging
import threading
from datetime import datetime
import traceback

import requests

from common import Settings, s3

__author__ = "Alex Ganin"


def download_feed(dirName, url, *args):
  """Downloads a real-time vehicle positions file to the local storage
  and then uploads it to S3 and removes from the local storage

  Args:
    dirName: the local directory where the file will be saved initially
    url: the URL to download the file from
    *args: placeholder for any additional arguments, unused
  """

  fName = datetime.now().strftime("%Y%m%d-%H%M%S.pb")
  r = requests.get(url)
  fPath = os.path.join(Settings.ProjPath, "pb", dirName, fName)
  with open(fPath, "wb") as handle:
    handle.write(r.content)

  try:
    # always use '/' as path separator in S3
    objKey = '/'.join(["pb", dirName, fName.replace('-', '/')])
    s3Mgr = s3.S3Mgr()
    s3Mgr.upload_file(fPath, objKey)
    os.remove(fPath)

  except Exception: # pylint: disable=broad-except
    AirLog = logging.getLogger("airflow.task")
    if AirLog:
      AirLog.warn("Error while saving the file %s to S3 and/or DB" % fPath)
      AirLog.warn(traceback.format_exc())
    pass # do not interfere with other threads that might succeed


def main():
  """Downloads the vehicle positions feed from MBTA
  12 times during 1 minute by running a new thread every 5 seconds
  """
  Feeds = [
      ("VehiclePos", "https://cdn.mbta.com/realtime/VehiclePositions.pb", 5)
  ]

  for feedTpl in Feeds:
    p = os.path.join(Settings.ProjPath, "pb", feedTpl[0])
    if not os.path.exists(p):
      os.makedirs(p)

  threads = []
  for sec in range(0, 59, 5):
    for feedTpl in Feeds:
      if sec % feedTpl[2] != 0:
        continue
      t = threading.Thread(target=download_feed, args=feedTpl)
      t.start()
      threads.append(t)
    if sec == 55:
      break
    time.sleep(5)

  for t in threads:
    t.join()

if __name__ == "__main__":
  main()
