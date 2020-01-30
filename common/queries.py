__author__ = "Alex Ganin"


Queries = {
"createVehPos": """

CREATE TABLE `VehPos` (
  `RouteId` char(50) DEFAULT NULL,
  `DT` DateTime NOT NULL,
  `VehicleId` char(50) NOT NULL,
  `TripId` char(50) NOT NULL,
  `Lat` float NOT NULL,
  `Lon` float NOT NULL,
  `Status` tinyint(4) DEFAULT NULL,
  `StopSeq` int(11) DEFAULT NULL,
  `StopId` char(50) DEFAULT NULL,
  UNIQUE KEY `unique_timetrip` (`DT`,`TripId`)
);
""",

"insertVehPos": """

INSERT IGNORE INTO `VehPos` (
  RouteId, DT, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
;
""",

"createVehPosPb": """

CREATE TABLE `VehPosPb`(
  `S3Key` char(50) Primary Key,
  `NumRecs` integer,
  `S3KeyDT` DateTime,
  `SDate` DateTime,
  `EDate` DateTime,
  `IsInVehPos` tinyint(1) DEFAULT '0'
);
""",

"insertVehPosPb": """

INSERT IGNORE INTO `VehPosPb` (
  S3Key, NumRecs, S3KeyDT, SDate, EDate
)
VALUES (%s, %s, %s, %s, %s)
;
""",

"selectVehPosPb_forDate" : """

SELECT S3Key
FROM VehPosPb
WHERE NumRecs > 0 and IsInVehPos and
      S3KeyDT > %s and S3KeyDT < %s 
;
""",

"createTU" : """

CREATE TABLE `TU` (
  `S3KeyDT` DateTime,
  `TripId` char(50),
  `SDate` Date,
  `StopId` char(50),
  `StopSeq` integer,
  `Arrival` DateTime,
  `Departure` DateTime
);
""",

"insertTU": """

INSERT INTO `TU` (
  S3KeyDT, TripId, SDate, StopId, StopSeq, Arrival, Departure
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
;
""",

"createS3Prefixes": """

CREATE TABLE `S3Prefixes` (
  `Prefix` char(50) PRIMARY KEY,
  `NumRecs` integer NOT NULL
);
""",

"insertS3Prefix": """

INSERT INTO `S3Prefixes` (
  Prefix, NumRecs
)
VALUES (%s, %s)
;
""",

"selectS3Prefixes": """

SELECT Prefix FROM `S3Prefixes`;
""",

"createPqDates": """

CREATE TABLE `PqDates` (
  `D` Date PRIMARY KEY,
  `NumKeys` integer NOT NULL,
  `NumRecs` integer NOT NULL,
  `IsInVPDelays` tinyint(1) DEFAULT '0'
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

SELECT D FROM `PqDates`
WHERE %s;
""",

"createVPDelays": """

CREATE TABLE `VPDelays` (
  `RouteId` char(50) DEFAULT NULL,
  `TripId` char(50) NOT NULL,
  `StopId` char(50) NOT NULL,
  `StopName` char(200) DEFAULT NULL,
  `StopLat` float NOT NULL,
  `StopLon` float NOT NULL,
  `SchedDT` DateTime NOT NULL,
  `EstDT` DateTime NOT NULL,
  `EstDist` float NOT NULL,
  `EstDelay` float NOT NULL
);
""",

"insertVPDelays": """

INSERT INTO `VPDelays` (
  RouteId, TripId, StopId, StopName, StopLat, StopLon, SchedDT,
  EstLat, EstLon, EstDT, EstDist, EstDelay
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
;
""",
}
