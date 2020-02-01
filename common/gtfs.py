from io import StringIO
from datetime import datetime

import csv

from . import utils, s3
from . import AppEx

class MBTA_ArchivedFeedDesc:
  def __init__(self):
    self.startDate = None
    self.endDate = None
    self.version = None
    self.s3Key = None
    self.url = None
    self.note = None

  def includesDate(self, d):
    return self.startDate <= d and d <= self.endDate

  def includesFiles(self, fileLst):
    s3Mgr = s3.S3Mgr()
    for fName in fileLst:
      if not s3Mgr.prefix_exists('/'.join(["GTFS", self.s3Key, fName])):
        return False
    return True


class MBTA_AchivedFeedsParser:
  def __init__(self, header):
    self.ixStartDate = utils.index_in_list(header, "feed_start_date")
    self.ixEndDate = utils.index_in_list(header, "feed_end_date")
    self.ixVersion = utils.index_in_list(header, "feed_version")
    self.ixUrl = utils.index_in_list(header, "archive_url")
    self.ixNote = utils.index_in_list(header, "archive_note")
    if self.ixStartDate < 0 or self.ixEndDate < 0 or self.ixUrl < 0 or \
       self.ixVersion < 0:
      raise AppEx("Required column is missing")

  def parse_row(self, row):
    ret = MBTA_ArchivedFeedDesc()
    ret.startDate = datetime.strptime(row[self.ixStartDate], "%Y%m%d").date()
    ret.endDate = datetime.strptime(row[self.ixEndDate], "%Y%m%d").date()
    ret.version = row[self.ixVersion]
    ret.s3Key = utils.replace_s3_invalid_characters(ret.version)
    ret.url = row[self.ixUrl]
    if self.ixNote >= 0:
      ret.note = row[self.ixNote]
    return ret

def read_feed_descs(byteStr):
  # parse first to check if we can work with this
  contentFile = StringIO(byteStr.decode("utf-8"))
  reader = csv.reader(contentFile, delimiter=',')
  parser = MBTA_AchivedFeedsParser(next(reader))
  feedDescs = []
  for row in reader:
    feedDescs.append(parser.parse_row(row))
  feedDescs.sort(key=lambda fd: fd.startDate, reverse=True)

  return feedDescs
