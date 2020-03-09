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
  """

  def __init__(self):
    """Initializes the instance"""

    self.AppName = "my best transit app"
    self.FooterLine = "Alex Ganin, Insight Data Engineering, Boston MA, Winter 2020"

    self.PyPath = os.path.abspath(os.path.dirname(__file__))
    self.ProjPath = os.path.join(self.PyPath, "..")

    self.PyVersion = sys.version_info[0]

    self.S3BucketName = "alxga-insde"

    self.MBTA_TZ = pytz.timezone("US/Eastern")


Settings = Settings()
