"""Helpers to parse GTFS Real-Time Protobuf files"""

from datetime import datetime
from google.protobuf.message import DecodeError
from . import s3, gtfs_realtime_pb2


def process_entities(data,
                     eachAlert=None, eachTripUpdate=None, eachVehiclePos=None):
  """Calls a given function for each entry in a Protobuf file

  Args:
    data: Protobuf file contents
    eachAlert: callback for every Alert entry
    eachTripUpdate: callback for every Trip Update entry
    eachVehiclePos: callback for every Vehicle Position entry
  """

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
  """Builds a Protobuf metadata tuple for the VehPosPb table

  Args:
    data: Protobuf file contents
  """
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
  """Builds a vehicle position tuple for the VehPos table

  Args:
    pbVal: Protobuf vehicle position entry
  """
  dt = datetime.utcfromtimestamp(pbVal.timestamp)
  return (
    pbVal.trip.route_id, dt, pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )


# use this to create a pyspark Dataframe
def vehpos_pb2_to_dbtpl_dtlocal(pbVal):
  """Builds a vehicle position tuple for a vehicle positions Parquet file

  Args:
    pbVal: Protobuf vehicle position entry
  """
  dt = datetime.fromtimestamp(pbVal.timestamp)
  return (
    pbVal.trip.route_id, dt, pbVal.vehicle.id, pbVal.trip.trip_id,
    pbVal.position.latitude, pbVal.position.longitude,
    pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
  )
