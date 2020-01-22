import os
from datetime import datetime
from google.protobuf.message import DecodeError
from dbconn import dbConn
import gtfs_realtime_pb2

PBDIRPATH = os.path.join("/home/alxga/data/mbta-VehiclePos")

dbConn.connect()
cursor = dbConn.cnx.cursor()

def process_alert(pbVal):
  pass

def process_trip_update(pbVal):
  pass

def process_vehicle_pos(pbVal):
  sqlStmt = """
    INSERT IGNORE into TVehPos(
      RouteId, TStamp, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
    )
    values(%s, %s, %s, %s, %s, %s, %s, %s, %s);
  """

  tStamp = datetime.fromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  tpl = (
    pbVal.trip.route_id, tStamp.strftime('%Y-%m-%d %H:%M:%S'),
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

  cursor.execute(sqlStmt, tpl)

fileCounter = 0
for entry in sorted(os.listdir(PBDIRPATH)):
  fileCounter += 1
  print("Processing file %d: %s" % (fileCounter, entry))

  pbPath = os.path.join(PBDIRPATH, entry)
  with open(pbPath, "rb") as f:
    message = gtfs_realtime_pb2.FeedMessage()
    try:
      message.ParseFromString(f.read())
    except DecodeError:
      continue

  for entity in message.entity:
    if entity.HasField('alert'):
      process_alert(entity.alert)
    if entity.HasField('trip_update'):
      process_trip_update(entity.trip_update)
    if entity.HasField('vehicle'):
      process_vehicle_pos(entity.vehicle)

  dbConn.cnx.commit()

cursor.close()
dbConn.close()
