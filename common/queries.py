"""Query strings"""

__author__ = "Alex Ganin"


Queries = {

"selectVehPosPb_forDate" : """

SELECT S3Key
FROM VehPosPb
WHERE NumRecs > 0 and IsInVehPos and
      S3KeyDT > %s and S3KeyDT < %s 
;
""",

"createPqDates": """

CREATE TABLE `PqDates` (
  `D` Date PRIMARY KEY,
  `NumKeys` integer NOT NULL,
  `NumRecs` integer NOT NULL,
  `IsInVPDelays` tinyint(1) DEFAULT '0',
  `IsInHlyDelays` tinyint(1) DEFAULT '0'
);
""",

"insertPqDate": """

INSERT INTO `PqDates` (
  D, NumKeys, NumRecs
)
VALUES (%s, %s, %s)
;
""",

"selectPqDatesWhere": """

SELECT D, IsInVPDelays, IsInHlyDelays FROM `PqDates`
WHERE %s
;
""",

"createVPDelays": """

CREATE TABLE `VPDelays` (
  `D` Date NOT NULL,
  `RouteId` char(50) DEFAULT NULL,
  `TripId` char(50) NOT NULL,
  `StopId` char(50) NOT NULL,
  `StopName` char(200) DEFAULT NULL,
  `StopLat` float NOT NULL,
  `StopLon` float NOT NULL,
  `SchedDT` DateTime NOT NULL,
  `EstLat` float NOT NULL,
  `EstLon` float NOT NULL,
  `EstDT` DateTime NOT NULL,
  `EstDist` float NOT NULL,
  `EstDelay` float NOT NULL
);
""",

"insertVPDelays": """

INSERT INTO `VPDelays` (
  D, RouteId, TripId, StopId, StopName, StopLat, StopLon, SchedDT,
  EstLat, EstLon, EstDT, EstDist, EstDelay
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
;
""",

"createHlyDelays": """

CREATE TABLE `HlyDelays` (
  `D` Date NOT NULL,
  `DateEST` Date NOT NULL,
  `HourEST` smallint NOT NULL,
  `RouteId` char(50) DEFAULT NULL,
  `StopName` char(200) DEFAULT NULL,
  `AvgDelay` float NOT NULL,
  `AvgDist` float NOT NULL,
  `Cnt` integer NOT NULL,
  `StopLat` float DEFAULT NULL,
  `StopLon` float DEFAULT NULL,
  `StopId` char(50) DEFAULT NULL
);
""",

"insertHlyDelays": """

INSERT INTO `HlyDelays` (
  D, DateEST, HourEST, RouteId, StopName, AvgDelay, AvgDist, Cnt,
  StopLat, StopLon, StopId
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
;
""",

"countApprox": """
SELECT table_rows "Rows Count"
FROM information_schema.tables
WHERE table_name='%s'
;
""",

"countWhere": """
SELECT count(*)
FROM `%s`
WHERE %s
;
""",
}
