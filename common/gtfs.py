"""Helpers to parse GTFS feeds from zip archives"""

from io import StringIO
from datetime import datetime

import csv

from . import utils, s3
from . import AppEx

class MBTA_ArchivedFeedDesc:
  """Describes an GTFS archive file

  Provides start and end date when the feed is in effect, version string from
  MBTA, S3 prefix of the feed, its URL on the MBTA website and note if any.
  """

  def __init__(self):
    """Initializes the instance
    """

    self.startDate = None
    self.endDate = None
    self.version = None
    self.s3Key = None
    self.url = None
    self.note = None


  def includes_date(self, d):
    """Returns whether the feed is in effect on a given date

    Args:
      dt: target date
    """

    return self.startDate <= d and d <= self.endDate


  def includes_files(self, fileLst):
    """Returns whether the feed files in S3 include given files

    Args:
      fileLst: file names to check, e.g., ['stops.txt', 'trips.txt']
    """

    s3Mgr = s3.S3Mgr()
    for fName in fileLst:
      if not s3Mgr.prefix_exists('/'.join(["GTFS", self.s3Key, fName])):
        return False
    return True


class MBTA_AchivedFeedsParser:
  """Parses a text document describing the feeds available from the MBTA
  """

  def __init__(self, header):
    """Initializes the instance

    Args:
      header: first row of the text document to be parsed

    Raises:
      AppEx: if the file format is not supported
    """

    self.ixStartDate = utils.index_in_list(header, "feed_start_date")
    self.ixEndDate = utils.index_in_list(header, "feed_end_date")
    self.ixVersion = utils.index_in_list(header, "feed_version")
    self.ixUrl = utils.index_in_list(header, "archive_url")
    self.ixNote = utils.index_in_list(header, "archive_note")
    if self.ixStartDate < 0 or self.ixEndDate < 0 or self.ixUrl < 0 or \
       self.ixVersion < 0:
      raise AppEx("Required column is missing")


  def parse_row(self, row):
    """Parse a line of input file

    Args:
      row: the line to be parsed

    Returns:
      an MBTA_ArchivedFeedDesc instance parsed from the line
    """

    ret = MBTA_ArchivedFeedDesc()
    ret.startDate = datetime.strptime(row[self.ixStartDate], "%Y%m%d").date()
    ret.endDate = datetime.strptime(row[self.ixEndDate], "%Y%m%d").date()
    ret.version = row[self.ixVersion]
    ret.s3Key = utils.replace_s3_invalid_characters(ret.version)
    ret.url = row[self.ixUrl]
    if self.ixNote >= 0:
      ret.note = row[self.ixNote]
    return ret

def read_feed_descs(bytesObj):
  """Parses an input file contents passed as a bytes object

  Args:
    byteStr: file contents as a bytes object

  Returns:
    a list of MBTA_ArchivedFeedDesc instances parsed from the input
  """

  # parse first to check if we can work with this
  contentFile = StringIO(bytesObj.decode("utf-8"))
  reader = csv.reader(contentFile, delimiter=',')
  parser = MBTA_AchivedFeedsParser(next(reader))
  feedDescs = []
  for row in reader:
    feedDescs.append(parser.parse_row(row))
  feedDescs.sort(key=lambda fd: fd.startDate, reverse=True)

  return feedDescs
