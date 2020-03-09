"""Generic helper functions"""

from datetime import datetime, timedelta
import pytz
from .settings import Settings

__author__ = "Alex Ganin"


def index_in_list(lst, val):
  """Returns the index of an element in a list

  Runs a linear search

  Args:
    lst: list where to search the element
    val: element to search for
  """

  i = 0
  for item in lst:
    if item == val:
      return i
    i += 1
  return -1


def replace_s3_invalid_characters(key):
  """Replaces characters invalid for an S3 object key in a string

  Args:
    key: string where to replace characters

  Returns:
    string where any invalid characters were replaced with underscores
  """

  spec_chars = " !-_'.,*()"
  lst = []
  for char in key:
    if char.isalpha() or char.isdigit() or spec_chars.find(char) >= 0:
      lst.append(char)
    else:
      lst.append('_')
  return ''.join(lst)


def daterange(start_date, end_date):
  """Returns an object for a range of datetimes to use in loops

  Datetimes increase in increments of 1 day

  Args:
    start_date: datetime where the range starts
    end_date: datetime where the range ends
  """

  deltadays = int((end_date - start_date).days)
  for n in range(deltadays):
    yield start_date + timedelta(n)


def sched_time_to_dt(timeStr, targetDate):
  """Converts a GTFS schedule time string to a datetime

  Note that a GTFS time may be more than 24 hours, in which case
  the function removes 24 from the hours part of the time and increases
  the date part of the datetime by 1

  Args:
    timeStr: time part of the datetime object
    targetDate: date part of the datetime object
  """

  if targetDate is None:
    targetDate = datetime.today()
  tkns = timeStr.split(':')
  h = int(tkns[0])
  if h > 23:
    h -= 24
    delta = timedelta(days=1)
  else:
    delta = timedelta(days=0)
  dt = datetime(
        year=targetDate.year, month=targetDate.month, day=targetDate.day,
        hour=h, minute=int(tkns[1]), second=int(tkns[2])
    ) + delta
  return dt

def dst_diff(targetDate):
  # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
  dt = datetime(targetDate.year, targetDate.month, targetDate.day, 8)
  dt1 = dt + timedelta(days=1)
  dt = pytz.utc.localize(dt).astimezone(Settings.MBTA_TZ)
  dt1 = pytz.utc.localize(dt1).astimezone(Settings.MBTA_TZ)
  return dt1.hour - dt.hour
