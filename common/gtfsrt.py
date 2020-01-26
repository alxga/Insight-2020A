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


def vehpos_pb2_to_dbtpl(pbVal):
  tStamp = datetime.utcfromtimestamp(pbVal.timestamp)
  # tStamp = tStamp.replace(tzinfo=timezone.utc).astimezone(tz=None)
  return (
    pbVal.trip.route_id, tStamp,
    pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )
