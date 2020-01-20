import os
import csv
import pickle as pkl
import dateutil.parser as dtparser
from gtfsstatic import utils, Settings

__author__ = "Alex Ganin"


csvVehPosDir = "/home/alxga/data/csv-mbta-VehiclePos"


class RowParser:
  def __init__(self, header):
    self.ixTimestamp = utils.index_in_list(header, "timestamp")
    self.ixGeo = utils.index_in_list(header, "geo")
    self.ixStatus = utils.index_in_list(header, "status")
    self.ixStopSeq = utils.index_in_list(header, "stop_sequence")
    self.ixVid = utils.index_in_list(header, "vid")

  def process_row(self, row):
    for i, val in enumerate(row):
      row[i] = utils.remove_enclosing_quotes(val.strip())
    row[self.ixGeo] = (float(row[self.ixGeo]), float(row[self.ixGeo + 1]))
    del row[self.ixGeo + 1]
    row[self.ixTimestamp] = dtparser.parse(row[self.ixTimestamp])
    row[self.ixStatus] = int(row[self.ixStatus])
    row[self.ixStopSeq] = int(row[self.ixStopSeq])
    return (row[self.ixTimestamp], row[self.ixVid])


def process_vp_csv(csvPath, rowsDict):
  with utils.open_csv_r(csvPath) as f:
    reader = csv.reader(f)
    header = next(reader)
    header = [x.strip() for x in header]
    parser = RowParser(header)
    for row in reader:
      k = parser.process_row(row)
      rowsDict[k] = row
    return header


def read_vehicle_pos():
  pklPath = os.path.join(Settings.ProjPath, "csv-mbta-VehiclePos.pkl")

  if os.path.exists(pklPath):
    with open(pklPath, "rb") as f:
      pickled = pkl.load(f)
      return pickled
  else:
    rowsDict = {}
    fileCount = 0

    for entry in os.listdir(csvVehPosDir):
      csvPath = os.path.join(csvVehPosDir, entry)
      if not os.path.isfile(csvPath):
        continue
      fileCount += 1
      fileName = os.path.split(csvPath)[1]
      print("%d: %s" % (fileCount, fileName))
      try:
        header = process_vp_csv(csvPath, rowsDict)
      except StopIteration:
        pass

    with open(pklPath, 'wb') as f:
      pkl.dump((header, rowsDict), f, 2)

    return (header, rowsDict)


Header, RowsDict = read_vehicle_pos()


def write_77():
  filtered = []
  for v in RowsDict.values():
    if v[0] == '77':
      filtered.append(v)

  filtered.sort(key=lambda row: row[1])
  outPath = os.path.join(Settings.ProjPath, "77.csv")
  with utils.open_csv_w(outPath) as outF:
    writer = csv.writer(outF)
    writer.writerow(Header)
    for row in filtered:
      writer.writerow(row)

write_77()
