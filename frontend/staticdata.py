# pylint: disable=invalid-name
# pylint: disable=usused-variable

import os
import csv
import urllib
from collections import namedtuple
from common import Settings
logger = Settings.ConsoleLogger


_RECONCILED_NAME = 'reconciled.csv'


_CsvRecord = namedtuple('_CsvRecord',
  'DateEST, HourEST, AvgDelay, AvgDist, Cnt'
)


class CsvParser:
  def __init__(self):
    self.ix_date = -1
    self.ix_hour = -1
    self.ix_delay = -1
    self.ix_dist = -1
    self.ix_cnt = -1

  def _init_from_header(self, header):
    self.ix_date = header.index('DateEST')
    self.ix_hour = header.index('HourEST')
    self.ix_delay = header.index('AvgDelay')
    self.ix_dist = header.index('AvgDist')
    self.ix_cnt = header.index('Cnt')
  
  def read(self, csv_path):
    ret = []
    with open(csv_path, 'r', newline='') as f:
      reader = csv.reader(f, delimiter=',')
      self._init_from_header(next(reader))
      for row in reader:
        ret.append(_CsvRecord(
          row[self.ix_date], int(row[self.ix_hour]),
          float(row[self.ix_delay]), float(row[self.ix_dist]),
          int(row[self.ix_cnt])
        ))
    return ret
      


def _reconcile_multiple_csvs(stop_dir, csv_names):
  """Combines CSV delay data files if there's more than one

  Those files result from minor stop name changes (e.g. casing)
  that still end up in the same output folder
  """
  data = []
  parser = CsvParser()
  for csv_name in csv_names:
    if csv_name == _RECONCILED_NAME:
      continue
    data.extend(parser.read(os.path.join(stop_dir, csv_name)))
  data.sort(key=lambda x: (x.DateEST, x.HourEST))

  if data:
    cleaned = [data[0]]
    for d in data[1:]:
      if cleaned[-1].DateEST == d.DateEST and cleaned[-1].HourEST == d.HourEST:
        wt1 = cleaned[-1].Cnt
        wt2 = d.Cnt
        cleaned[-1] = _CsvRecord(d.DateEST, d.HourEST,
          (cleaned[-1].AvgDelay * wt1 + d.AvgDelay * wt2) / (wt1 + wt2),
          (cleaned[-1].AvgDist * wt1 + d.AvgDist * wt2) / (wt1 + wt2),
          cleaned[-1].Cnt + d.Cnt
        )
      else:
        cleaned.append(d)
    logger.info((
      f'Reconciled dataset has {len(cleaned)} out of '
      f'{len(data)} original rows for {stop_dir}'
    ))
    data = cleaned

  with open(os.path.join(stop_dir, _RECONCILED_NAME), 'w', newline='') as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerow(['DateEST', 'HourEST', 'AvgDelay', 'AvgDist', 'Cnt'])
    for d in data:
      writer.writerow([d.DateEST, d.HourEST, d.AvgDelay, d.AvgDist, d.Cnt])
  logger.info(f'Combined successfully multiple CSV files for {stop_dir}')


def _read_static_hourly_data(dir_path):
  route_dirs = [
    d for d in os.listdir(dir_path) if \
      os.path.isdir(os.path.join(dir_path, d))
  ]
  combo_count = 0
  routes_dct = {}
  for rd in route_dirs:
    if rd.startswith("Route="):
      route = urllib.parse.unquote(rd[6:])
      if route in routes_dct:
        raise Exception(f'Duplicate route {route}')
      route_dir = os.path.join(dir_path, rd)
      stop_dirs = [
        d for d in os.listdir(route_dir) if \
          os.path.isdir(os.path.join(route_dir, d))
      ]
      stops_dct = {}
      for sd in stop_dirs:
        if sd.startswith('Stop='):
          stop = urllib.parse.unquote(sd[5:])
          if stop in stops_dct:
            raise Exception(f'Duplicate stop {stop} for route {route}')
          stop_dir = os.path.join(route_dir, sd)
          csv_names = [
            d for d in os.listdir(stop_dir) if \
              os.path.isfile(os.path.join(stop_dir, d)) and d[-4:].lower() == '.csv'
          ]
          if len(csv_names) > 1:
            if not os.path.exists(os.path.join(stop_dir, _RECONCILED_NAME)):
              _reconcile_multiple_csvs(stop_dir, csv_names)
            stops_dct[stop] = os.path.join(stop_dir, _RECONCILED_NAME)
          elif len(csv_names) < 1:
            raise Exception(f'No CSV files for {route}:::[{stop}]')
          else:
            stops_dct[stop] = os.path.join(stop_dir, csv_names[0])
      combo_count += len(stops_dct)
      routes_dct[route] = stops_dct
  logger.info(
    f'Parsed {len(routes_dct)} routes and {combo_count} route/stop pairs'
  )
  return routes_dct, combo_count


StaticData = None


def init_static_data(dir_path):
  global StaticData
  StaticData = {}
  StaticData['routes_dct'], _ = _read_static_hourly_data(dir_path)


def main():
  routes_dct, combo_count = \
    _read_static_hourly_data('C:\\Users\\alexa\\Desktop\\HlyDelays0-CSV')

  parser = CsvParser()
  cnt_valid = 0
  for route, stops_dct in routes_dct.items():
    for stop, csv_path in stops_dct.items():
      data = parser.read(csv_path)
      for ix, d in enumerate(data[1:]):
        prev = data[ix]
        if prev.DateEST > d.DateEST:
          raise Exception(f'Invalid data order for {route}:::[{stop}]')
        elif prev.DateEST == d.DateEST and prev.HourEST >= d.HourEST:
          raise Exception(f'Invalid data order for {route}:::[{stop}]')
      cnt_valid += 1
      if cnt_valid % 100 == 0:
        logger.info(f'Validated {cnt_valid} of {combo_count} files')


if __name__ == '__main__':
  main()
