from enum import Enum
from datetime import datetime
import dateutil.parser as dtparser
from appex import AppEx
import utils
from settings import Settings
import geo

class Flags:
  Mon = 1<<0
  Tue = 1<<1
  Wed = 1<<2
  Thu = 1<<3
  Fri = 1<<4
  Sat = 1<<5
  Sun = 1<<6


class CalendarService:
  def __init__(self):
    self.id = None
    self.days = 0
    self.startDate = None
    self.endDate = None
    self.exceptions = []

class CalendarCsvParser:
  def __init__(self, header):
    self.ixId = utils.index_in_list(header, "service_id")
    if self.ixId < 0:
      raise AppEx("Required column is missing")
    self.ixDays = []
    self.dayFlags = [Flags.Mon, Flags.Tue, Flags.Wed, Flags.Thu,
                     Flags.Fri, Flags.Sat, Flags.Sun]
    names = ["monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday"]
    for name in names:
      self.ixDays.append(utils.index_in_list(header, name))
      if self.ixDays[-1] < 0:
        raise AppEx("Required column is missing")
    self.ixStartDate = utils.index_in_list(header, "start_date")
    self.ixEndDate = utils.index_in_list(header, "end_date")
    if self.ixStartDate < 0 or self.ixEndDate < 0:
      raise AppEx("Required column is missing")

  def parse_row(self, row):
    ret = CalendarService()
    ret.id = row[self.ixId]
    ret.days = 0
    for i, flag in enumerate(self.dayFlags):
      if row[self.ixDays[i]] == '1':
        ret.days |= flag
    ret.startDate = dtparser.parse(row[self.ixStartDate])
    ret.endDate = dtparser.parse(row[self.ixEndDate])
    return ret


class CalendarServiceException:

  class Type(Enum):
    ADDED = 1
    REMOVED = 2

  def __init__(self):
    self.type = None
    self.date = None
    self.holiday = None

class CalendarDatesCsvParser:
  def __init__(self, header, serviceDict):
    self.serviceDict = serviceDict
    self.ixId = utils.index_in_list(header, "service_id")
    self.ixDate = utils.index_in_list(header, "date")
    self.ixType = utils.index_in_list(header, "exception_type")
    self.ixHolidayName = utils.index_in_list(header, "holiday_name")
    if self.ixId < 0 or self.ixDate < 0 or self.ixType < 0:
      raise AppEx("Required column is missing")

  def parse_row(self, row):
    serviceId = row[self.ixId]
    service = self.serviceDict[serviceId]
    serviceEx = CalendarServiceException()

    serviceEx.type = CalendarServiceException.Type(int(row[self.ixType]))
    serviceEx.date = dtparser.parse(row[self.ixDate])
    if self.ixHolidayName >= 0:
      serviceEx.holiday = row[self.ixHolidayName]

    service.exceptions.append(serviceEx)


class Stop:
  class LocType(Enum):
    STOP = 0
    STATION = 1
    ENTR_EXIT = 2
    GENERIC = 3
    BOARDING_AREA = 4


  def __init__(self):
    self.id = None
    self.code = None
    self.name = None
    self.desc = None
    self.coords = None
    self.locationType = 0
    self.parentStation = None
    self.wheelchairBoarding = 0

  def isWheelchairAccessible(self):
    if self.parentStation is None:
      rets = [None, True, False]
      return rets[self.wheelchairBoarding]

    if self.wheelchairBoarding == 0:
      return self.parentStation.isWheelchairAccessible()

    return self.wheelchairBoarding == 1

class StopCsvParser:
  def __init__(self, header):
    self.ixId = utils.index_in_list(header, "stop_id")
    self.ixCode = utils.index_in_list(header, "stop_code")
    self.ixName = utils.index_in_list(header, "stop_name")
    self.ixDesc = utils.index_in_list(header, "stop_desc")
    self.ixLat = utils.index_in_list(header, "stop_lat")
    self.ixLon = utils.index_in_list(header, "stop_lon")
    self.ixLocationType = utils.index_in_list(header, "location_type")
    self.ixParentStation = utils.index_in_list(header, "parent_station")
    self.ixWheelchairBoarding = utils.index_in_list(header,
                                                    "wheelchair_boarding")

    if self.ixId < 0 or self.ixCode < 0 or self.ixName < 0 or \
       self.ixDesc < 0 or self.ixLat < 0 or self.ixLon < 0 or \
       self.ixParentStation < 0 or self.ixWheelchairBoarding < 0:
      raise AppEx("Required column is missing")

  def parse_row(self, row):
    ret = Stop()

    ret.id = row[self.ixId]
    ret.code = row[self.ixCode]
    ret.name = row[self.ixName]
    ret.desc = row[self.ixDesc]

    if self.ixLocationType >= 0:
      ret.locationType = Stop.LocType(int(row[self.ixLocationType]))
    else:
      ret.locationType = Stop.LocType.STOP

    try:
      ret.coords = geo.Coords(float(row[self.ixLat]), float(row[self.ixLon]))
    except ValueError:
      if ret.locationType == Stop.LocType.STOP or \
         ret.locationType == Stop.LocType.STATION or \
         ret.locationType == Stop.LocType.ENTR_EXIT:
        raise AppEx("Missing required coordinates for a stop")
      else:
        ret.coords = None

    ret.parentStation = row[self.ixParentStation]
    ret.wheelchairBoarding = int(row[self.ixWheelchairBoarding])

    return ret

  def resolve_forward_refs(self, stopDict):
    for stop in stopDict.values():
      if stop.parentStation:
        stop.parentStation = stopDict[stop.parentStation]
      else:
        stop.parentStation = None
