import os
import sys
import shutil
import platform

__author__ = "Alex"


class Settings:
  def __init__(self):
    """Initializes the instance"""

    self.SrcDirPath = os.path.abspath(os.path.dirname(__file__))
    
    self.ProjPath = None
    arg1 = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg1.startswith("-projPath=") and os.path.isdir(arg1[10:]):
      self.ProjPath = arg1[10:]
    else:
      self.ProjPath = os.path.join(self.SrcDirPath, "..")

    self.IOPath = self.ProjPath
    self.GTFSStaticPath = None

    self.IsCygwin = platform.system()[:6] == "CYGWIN"
    self.PyVersion = sys.version_info[0]

    self.CsvVehPos = os.path.join(self.ProjPath, "csv-mbta-VehiclePos")


Settings = Settings()
