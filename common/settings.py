import os
import sys
import platform

__author__ = "Alex Ganin"


class Settings:
  def __init__(self):
    """Initializes the instance"""

    self.AppName = "MBTA: My Best Transit App"
    self.FooterLine = "Alex Ganin, Insight Data Engineering, Boston MA, Winter 2020"

    self.PyPath = os.path.abspath(os.path.dirname(__file__))
    self.ProjPath = os.path.join(self.PyPath, "..")

    self.IsCygwin = platform.system()[:6] == "CYGWIN"
    self.PyVersion = sys.version_info[0]

    self.S3BucketName = "alxga-insde"


Settings = Settings()
