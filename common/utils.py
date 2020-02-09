from datetime import datetime, timedelta
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

def rolling_weighted_triangle_conv(x, w, window_size):
    """Smooth with triangle window, also using per-element weights."""
    # Simplify slicing
    wing = window_size // 2

    # Pad both arrays with mirror-image values at edges
    xp = np.concatenate(( x[wing-1::-1], x, x[:-wing-1:-1] ))
    wp = np.concatenate(( w[wing-1::-1], w, w[:-wing-1:-1] ))

    # Generate a (triangular) window of weights to slide
    incr = 1. / (wing + 1)
    ramp = np.arange(incr, 1, incr)
    triangle = np.r_[ramp, 1.0, ramp[::-1]]

    D = np.convolve(wp*xp, triangle)[window_size-1:-window_size+1]
    N = np.convolve(wp, triangle)[window_size-1:-window_size+1]    
    return D/N
