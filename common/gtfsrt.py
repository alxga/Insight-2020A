from datetime import datetime
from google.protobuf.message import DecodeError
from . import s3, gtfs_realtime_pb2


def process_entities(data,
                     eachAlert=None, eachTripUpdate=None, eachVehiclePos=None):

  message = gtfs_realtime_pb2.FeedMessage()

  try:
    message.ParseFromString(data)
  except DecodeError:
    return

  for entity in message.entity:
    if eachAlert and entity.HasField('alert'):
      eachAlert(entity.alert)

    if eachTripUpdate and entity.HasField('trip_update'):
      eachTripUpdate(entity.trip_update)

    if eachVehiclePos and entity.HasField('vehicle'):
      eachVehiclePos(entity.vehicle)


def vehpospb_pb2_to_dbtpl(objKey, data):
  dts = []
  process_entities(data,
      eachVehiclePos=lambda x:
          dts.append(datetime.utcfromtimestamp(x.timestamp))
  )
  kdt = s3.S3FeedKeyDT(objKey)
  mn = min(dts, default=None)
  mx = max(dts, default=None)
  return (objKey, len(dts), kdt, mn, mx)


# use this with mysql.connector
def vehpos_pb2_to_dbtpl_dtutc(pbVal):
  dt = datetime.utcfromtimestamp(pbVal.timestamp)
  return (
    pbVal.trip.route_id, dt, pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )

# use this to create a pyspark Dataframe
def vehpos_pb2_to_dbtpl_dtlocal(pbVal):
  dt = datetime.fromtimestamp(pbVal.timestamp)
  return (
    pbVal.trip.route_id, dt, pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )
