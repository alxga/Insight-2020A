from datetime import datetime, timedelta
from . import Settings

__author__ = "Alex Ganin"


def open_csv_r(filePath):
  if Settings.PyVersion >= 3:
    return open(filePath, "rt", newline="")
  else:
    return open(filePath, "rb")


def open_csv_w(filePath):
  if Settings.PyVersion >= 3:
    return open(filePath, "wt", newline="")
  else:
    return open(filePath, "wb")


def index_in_list(lst, val):
  i = 0
  for item in lst:
    if item == val:
      return i
    i += 1
  return -1


def msec(tDelta):
  return int(tDelta.total_seconds() * 1000 + 0.5)


def remove_enclosing_quotes(txt):
  if len(txt) >= 2 and txt[0] == '\'' and txt[-1] == '\'':
    return txt[1:-1]
  else:
    return txt

def replace_s3_invalid_characters(key):
  spec_chars = " !-_'.,*()"
  lst = []
  for char in key:
    if char.isalpha() or char.isdigit() or spec_chars.find(char) >= 0:
      lst.append(char)
    else:
      lst.append('_')
  return ''.join(lst)


def daterange(start_date, end_date):
  deltadays = int((end_date - start_date).days)
  for n in range(deltadays):
    yield start_date + timedelta(n)


def sched_time_to_dt(timeStr, targetDate):
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
