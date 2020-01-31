import os
import time
import threading
from datetime import datetime
import traceback

import requests
import mysql.connector

from common import credentials
from common import Settings, s3, gtfsrt
from common.queries import Queries

__author__ = "Alex Ganin"


def push_vehpospb_dbtpl(tpl):
  cnx = None
  cursor = None
  sqlStmt = Queries["insertVehPosPb"]

  try:
    cnx = mysql.connector.connect(**credentials.MySQLConnArgs)
    cursor = cnx.cursor()
    # cursor.execute(sqlStmt, tpl)
    print("Skipped DB Push")
    cnx.commit()
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()


def download_feed(dirName, url, *args):
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

    if dirName[0:3] == "Veh": # TODO: this is hackish
      tpl = gtfsrt.vehpospb_pb2_to_dbtpl(objKey, r.content)
      push_vehpospb_dbtpl(tpl)

  except Exception: # pylint: disable=broad-except
    print("Error while saving the file %s to S3 and/or DB" % fPath)
    print(traceback.format_exc())
    pass


def main():
  Feeds = [
      ("T_VehiclePos", "https://cdn.mbta.com/realtime/VehiclePositions.pb", 5),
      ("T_TripUpdates", "https://cdn.mbta.com/realtime/TripUpdates.pb", 60),
      ("T_Alerts", "https://cdn.mbta.com/realtime/Alerts.pb", 30)
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
    time.sleep(5)

  for t in threads:
    t.join()

  print("Completed Successfully")

if __name__ == "__main__":
  main()
