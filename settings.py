import os
import sys
import shutil
import platform

__author__ = "Alex"


class Settings:
  def __init__(self):
    self.PyPath = os.path.abspath(os.path.dirname(__file__))

    self.IsCygwin = platform.system()[:6] == "CYGWIN"

    self.PyVersion = sys.version_info[0]

    self.CsvVehPos = os.path.join(self.PyPath, "csv-mbta-VehiclePos")


Settings = Settings()
