import os
import csv
from datetime import datetime
import dateutil.parser as dtparser
import pickle as pkl
import utils
from settings import Settings


RowsDict = {}


class RowParser:
  def __init__(self, header):
    self.ixTimestamp = utils.index_in_list(header, "timestamp")
    self.ixGeo = utils.index_in_list(header, "geo")
    self.ixStatus = utils.index_in_list(header, "status")
    self.ixStopSeq = utils.index_in_list(header, "stop_sequence")    
    self.ixVid = utils.index_in_list(header, "vid")

  def process_row(self, row):
    for i,str in enumerate(row):
      row[i] = utils.remove_enclosing_quotes(str.strip())
    row[self.ixGeo] = (float(row[self.ixGeo]), float(row[self.ixGeo + 1]))
    del row[self.ixGeo + 1]
    row[self.ixTimestamp] = dtparser.parse(row[self.ixTimestamp])
    row[self.ixStatus] = int(row[self.ixStatus])
    row[self.ixStopSeq] = int(row[self.ixStopSeq])
    return (row[self.ixTimestamp], row[self.ixVid])


def read_vp_csv(csvPath):
  with utils.open_csv_r(csvPath) as f:
    reader = csv.reader(f)
    header = next(reader)
    header = [x.strip() for x in header]
    parser = RowParser(header)  
    for row in reader:
      k = parser.process_row(row)
      RowsDict[k] = row
    return header


pklPath = os.path.join(Settings.PyPath, "csv-mbta-VehiclePos.pkl")

if os.path.exists(pklPath):
  with open(pklPath, "rb") as f:
    pickled = pkl.load(f)
    header = pickled[0]
    RowsDict = pickled[1]
else:
  fileCount = 0

  for entry in os.listdir(Settings.CsvVehPos):
    csvPath = os.path.join(Settings.CsvVehPos, entry)
    if not os.path.isfile(csvPath):
      continue
    header = read_vp_csv(csvPath)
    fileCount += 1
    print("%d: %s" % (fileCount, os.path.split(csvPath)[1]))
    if len(RowsDict) > 10000:
      break

  with open(pklPath, 'wb') as f:
    pkl.dump((header, RowsDict), f, 2)

filtered = []
for v in RowsDict.values():
  if v[0] == '77':
    filtered.append(v)

filtered.sort(key=lambda row: row[1])
outPath = os.path.join(Settings.PyPath, "77.csv")
with utils.open_csv_w(outPath) as outF:
  writer = csv.writer(outF)
  writer.writerow(header)
  for row in filtered:
    writer.writerow(row)
