CREATE TABLE `TVehPos` (
  `RouteId` char(50) DEFAULT NULL,
  `TStamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `VehicleId` char(50) NOT NULL,
  `TripId` char(50) NOT NULL,
  `Lat` float NOT NULL,
  `Lon` float NOT NULL,
  `Status` tinyint(4) DEFAULT NULL,
  `StopSeq` int(11) DEFAULT NULL,
  `StopId` char(50) DEFAULT NULL,
  UNIQUE KEY `unique_timetrip` (`TStamp`,`TripId`));
