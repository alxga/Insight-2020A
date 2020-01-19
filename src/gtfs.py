from datetime import datetime
import dateutil.parser as dtparser
from appex import AppEx
import utils
from settings import Settings

class Flags:
  Mon = 1<<0
  Tue = 1<<1
  Wed = 1<<2
  Thu = 1<<3
  Fri = 1<<4
  Sat = 1<<5
  Sun = 1<<6


SVC_ADDED = 1
SVC_REMOVED = 2


class CalendarService:
  def __init__(self):
    self.id = ""
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
  def __init__(self):
    self.type = 0
    self.date = None
    self.holiday = None

class CalendarDatesCsvParser:
  def __init__(self, header, serviceDict):
    self.serviceDict = serviceDict
    self.ixId = utils.index_in_list(header, "service_id")
    self.ixDate = utils.index_in_list(header, "date")
    self.ixExType = utils.index_in_list(header, "exception_type")
    self.ixHolidayName = utils.index_in_list(header, "holiday_name")
    if self.ixId < 0 or self.ixDate < 0 or self.ixExType < 0:
      raise AppEx("Required column is missing")

  def parse_row(self, row):
    serviceId = row[self.ixId]
    service = self.serviceDict[serviceId]
    serviceEx = CalendarServiceException()

    serviceEx.type = int(row[self.ixExType])
    serviceEx.date = dtparser.parse(row[self.ixDate])
    if self.ixHolidayName >= 0:
      serviceEx.holiday = row[self.ixHolidayName]
    if serviceEx.type != SVC_ADDED and serviceEx.type != SVC_REMOVED:
      raise AppEx("Invalid value for service exception type")

    service.exceptions.append(serviceEx)
