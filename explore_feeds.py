# pylint: disable=unused-import

import os
import sys
from datetime import datetime, timedelta

import mysql.connector

from common import credentials
from common import Settings, s3, utils, gtfsrt
from common.queries import Queries

s3Mgr = s3.S3Mgr()
keys = s3Mgr.fetch_keys("pb/TripUpdates/20200117", 100)

def explore_alert(alert):
  print("DEBUG ME")

def explore_trip_update(tu):
  print("DEBUG ME")

for key in keys:
  data = s3Mgr.fetch_object_body(key)
  gtfsrt.process_entities(
    data, eachAlert=explore_alert, eachTripUpdate=explore_trip_update
  )
