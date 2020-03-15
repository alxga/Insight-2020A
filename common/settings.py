"""Application configuration"""

import os
import sys
import pytz

__author__ = "Alex Ganin"


class Settings:
  """Class to hold settings and paths

  Attributes:
    AppName: Application name
    FooterLine: Frontend footer line
    PyPath: computed path to the current script folder
    ProjPath: project path, defaults to <PyPath>/..
    PyVersion: major version of the Python interpreter (2 or 3)
    S3BucketName: S3 bucket name
    InsertVPDelays: whether to update the VPDelays table
    MBTA_TZ: time zone of MBTA GTFS feeds
    GTFS_ObsoleteAfterDays: number of days between the latest delays
      calculation and the last day of a GTFS feed coverage for the feed to
      be considered obsolete and to be stored compressed in S3
  """

  def __init__(self):
    """Initializes the instance"""

    self.AppName = "my best transit app"
    self.FooterLine = "Alex Ganin, Insight Data Engineering, Boston MA, Winter 2020"

    self.PyPath = os.path.abspath(os.path.dirname(__file__))
    self.ProjPath = os.path.join(self.PyPath, "..")

    self.PyVersion = sys.version_info[0]

    self.S3BucketName = "alxga-insde"

    self.InsertVPDelays = False

    self.MBTA_TZ = pytz.timezone("US/Eastern")

    self.GTFS_ObsoleteAfterDays = 10


Settings = Settings()
