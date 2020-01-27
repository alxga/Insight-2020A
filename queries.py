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

INSERT IGNORE INTO VehPos(
  RouteId, DT, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
;
""",

"createVehPosPb": """

CREATE TABLE VehPosPb(
  S3Key char(50) Primary Key,
  NumRecs integer,
  S3KeyDT DateTime,
  SDate DateTime,
  EDate DateTime,
  IsInVehPos tinyint(1) DEFAULT '0'
);
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

"updateVehPosPb_setIsInVehPosMsk" : """

UPDATE VehPosPb
SET IsInVehPos = TRUE
WHERE S3Key = '%s'
;
""",

"createTU" : """

CREATE TABLE `TU`(
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

INSERT INTO TU(S3KeyDT, TripId, SDate, StopId, StopSeq, Arrival, Departure)
VALUES (%s, %s, %s, %s, %s, %s, %s)
;
""",
}
