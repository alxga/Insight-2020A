from enum import Enum

__author__ = "Alex Ganin"


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
    self.days = None
    self.startDate = None
    self.endDate = None
    self.exceptions = []


class CalendarServiceException:

  class Type(Enum):
    ADDED = 1
    REMOVED = 2

  def __init__(self):
    self.type = None
    self.date = None
    self.holiday = None


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
    self.locationType = None
    self.parentStation = None
    self.wheelchairBoarding = None

  def isWheelchairAccessible(self):
    if self.parentStation is None:
      rets = [None, True, False]
      return rets[self.wheelchairBoarding] # pylint: disable=invalid-sequence-index

    if self.wheelchairBoarding == 0:
      return self.parentStation.isWheelchairAccessible()

    return self.wheelchairBoarding == 1


class Route:
  class Type(Enum):
    TRAM = 0
    SUBWAY = 1
    RAIL = 2
    BUS = 3
    FERRY = 4
    CABLECAR = 5
    SUSPENDED = 6
    FUNICULAR = 7

  def __init__(self):
    self.id = None
    self.shortName = None
    self.longName = None
    self.desc = None
    self.routeType = None
    self.color = None
    self.textColor = None
