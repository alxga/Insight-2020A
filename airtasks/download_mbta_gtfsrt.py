import os
import time
import threading
from datetime import datetime
import traceback

import requests
import mysql.connector

from common import credentials
from common import Settings, s3
from common.queries import Queries

__author__ = "Alex Ganin"


def push_vehpospb_dbtpl(tpl):
  """Inserts a record into the VehPosPb table

  Args:
    tpl: a tuple to save into the database
  """

  cnx = None
  cursor = None
  sqlStmt = Queries["insertVehPosPb"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    cursor.execute(sqlStmt, tpl)
    cnx.commit()
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()


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

    # Attempt to push records to the VehPosPb table when we're reading
    # the vehicle positions feed
    # This can be used to speed up the appearance of data in VehPos,
    # but is disabled for now

    # if dirName[0:3] == "Veh":
    #   tpl = gtfsrt.vehpospb_pb2_to_dbtpl(objKey, r.content)
    #   push_vehpospb_dbtpl(tpl)

  except Exception: # pylint: disable=broad-except
    print("Error while saving the file %s to S3 and/or DB" % fPath)
    print(traceback.format_exc())
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
