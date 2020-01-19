import dateutil.parser as dtparser
from appex import AppEx
import utils
import geo
from gtfs import Flags, CalendarService, CalendarServiceException, Stop, \
                 Route


class CalendarCsv:
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
      raise AppEx("Required column is missing in the Calendar table")

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


class CalendarDatesCsv:
  def __init__(self, header, serviceDict):
    self.serviceDict = serviceDict
    self.ixId = utils.index_in_list(header, "service_id")
    self.ixDate = utils.index_in_list(header, "date")
    self.ixType = utils.index_in_list(header, "exception_type")
    self.ixHolidayName = utils.index_in_list(header, "holiday_name")
    if self.ixId < 0 or self.ixDate < 0 or self.ixType < 0:
      raise AppEx("Required column is missing in the Calendar Dates table")

  def parse_row(self, row):
    serviceId = row[self.ixId]
    service = self.serviceDict[serviceId]
    serviceEx = CalendarServiceException()

    serviceEx.type = CalendarServiceException.Type(int(row[self.ixType]))
    serviceEx.date = dtparser.parse(row[self.ixDate])
    if self.ixHolidayName >= 0:
      serviceEx.holiday = row[self.ixHolidayName]
    else:
      serviceEx.holiday = ""

    service.exceptions.append(serviceEx)


class StopsCsv:
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
      raise AppEx("Required column is missing in the Stops table")

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


class RoutesCsv:
  def __init__(self, header):
    self.ixId = utils.index_in_list(header, "route_id")
    self.ixShortName = utils.index_in_list(header, "route_short_name")
    self.ixLongName = utils.index_in_list(header, "route_long_name")
    self.ixDesc = utils.index_in_list(header, "route_desc")
    self.ixRouteType = utils.index_in_list(header, "route_type")
    self.ixColor = utils.index_in_list(header, "route_color")
    self.ixTextColor = utils.index_in_list(header, "route_text_color")

    if self.ixId < 0 or \
       (self.ixShortName < 0 and self.ixLongName < 0) or \
       self.ixRouteType < 0:
      raise AppEx("Required column is missing in the Routes table")

  def parse_row(self, row):
    ret = Route()
    ret.id = row[self.ixId]
    ret.shortName = row[self.ixShortName] if self.ixShortName >= 0 else ""
    ret.longName = row[self.ixLongName] if self.ixLongName >= 0 else ""
    ret.desc = row[self.ixDesc] if self.ixDesc >= 0 else ""
    ret.routeType = Route.Type(int(row[self.ixRouteType]))
    if self.ixColor >= 0:
      ret.color = int(row[self.ixColor], 16)
    else:
      ret.color = 0xFFFFFF
    if self.ixTextColor >= 0:
      ret.textColor = int(row[self.ixTextColor], 16)
    else:
      ret.textColor = 0xFFFFFF
    return ret
