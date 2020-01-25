from datetime import timedelta, date
from . import Settings

__author__ = "Alex Ganin"


def open_csv_r(_file_path):
  if Settings.PyVersion >= 3:
    return open(_file_path, "rt", newline="")
  else:
    return open(_file_path, "rb")


def open_csv_w(_file_path):
  if Settings.PyVersion >= 3:
    return open(_file_path, "wt", newline="")
  else:
    return open(_file_path, "wb")


def index_in_list(lst, val):
  i = 0
  for item in lst:
    if item == val:
      return i
    i += 1
  return -1


def msec(_dt):
  return int(_dt.total_seconds() * 1000 + 0.5)


def remove_enclosing_quotes(txt):
  if len(txt) >= 2 and txt[0] == '\'' and txt[-1] == '\'':
    return txt[1:-1]
  else:
    return txt

def daterange(start_date, end_date):
  deltadays = int((end_date - start_date).days)
  for n in range(deltadays):
    yield start_date + timedelta(n)
