"""Helpers to parse GTFS Real-Time Protobuf files"""

from google.protobuf.message import DecodeError
from . import gtfs_realtime_pb2


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
