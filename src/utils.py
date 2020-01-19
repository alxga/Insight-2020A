import sys
import os
import csv
import numpy as np
from settings import Settings

__author__ = "Alex"


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


def remove_enclosing_quotes(str):
  if len(str) >= 2 and str[0] == '\'' and str[-1] == '\'':
    return str[1:-1]
  else:
    return str

