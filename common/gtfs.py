from datetime import datetime

from . import utils
from . import AppEx

class MBTA_ArchivedFeedDesc:
  def __init__(self):
    self.startDate = None
    self.endDate = None
    self.version = None
    self.s3Key = None
    self.url = None
    self.note = None


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
