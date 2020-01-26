Queries = {
"createVehPos": """

CREATE TABLE `VehPos` (
  `RouteId` char(50) DEFAULT NULL,
  `Dt` Date NOT NULL,
  `Tm` Time NOT NULL,
  `VehicleId` char(50) NOT NULL,
  `TripId` char(50) NOT NULL,
  `Lat` float NOT NULL,
  `Lon` float NOT NULL,
  `Status` tinyint(4) DEFAULT NULL,
  `StopSeq` int(11) DEFAULT NULL,
  `StopId` char(50) DEFAULT NULL,
  UNIQUE KEY `unique_timetrip` (`DT`,`TripId`))
;
""",

"insertVehPos": """

INSERT IGNORE INTO VehPos(
  RouteId, Dt, Tm, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
;
""",

"createVehPosPb": """

CREATE TABLE VehPosPb(
  S3Key char(50) Primary Key,
  NumRecs integer,
  S3KeyDT DateTime,
  SDate DateTime,
  EDate DateTime)
;
""",

"insertVehPosPb": """

INSERT IGNORE INTO VehPosPb(S3Key, NumRecs, S3KeyDT, SDate, EDate)
VALUES (%s, %s, %s, %s, %s)
;
""",

"selectVehPosPb_toAddVehPos" : """

SELECT S3Key
FROM VehPosPb
WHERE NumRecs > 0 and not IsInVehPos
;
""",
}
