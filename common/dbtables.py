from datetime import datetime

from . import s3
from . import gtfsrt

def create_if_not_exists(conn, cls):
  if not conn.table_exists(cls.TABLE_NAME):
    conn.execute(cls.CREATE_STMT)
    conn.commit()


class S3Prefixes:
  """S3Prefixes table helper class
  """

  TABLE_NAME = "S3Prefixes"
  CREATE_STMT = """
    CREATE TABLE `S3Prefixes` (
      `Prefix` char(50) PRIMARY KEY,
      `NumRecs` integer NOT NULL
    );
  """


  @staticmethod
  def insertValues(conn, prefix, numRecs):
    """Inserts a record in the table

    Args:
      conn: a DBConn instance
      prefix, numRecs: Prefix (e.g., '20200115/10') and NumRecs
        to insert into the table
    """

    sqlStmt = """
      INSERT INTO `S3Prefixes` (
        Prefix, NumRecs
      )
      VALUES (%s, %s);
    """
    conn.execute(sqlStmt, (prefix, numRecs))


  @staticmethod
  def selectPrefixesDict(conn):
    """Returns a dictionary of all prefixes from the database table

    Args:
      conn: a DBConn instance
    """

    sqlStmt = """
      SELECT Prefix FROM `S3Prefixes`;
    """
    ret = {}
    cur = conn.execute(sqlStmt)
    for row in cur:
      ret[row[0]] = 1
    return ret


class VehPosPb:
  """VehPosPb table helper class
  """

  TABLE_NAME = "VehPosPb"
  CREATE_STMT = """
    CREATE TABLE `VehPosPb`(
      `S3Key` char(50) Primary Key,
      `NumRecs` integer,
      `S3KeyDT` DateTime,
      `SDate` DateTime,
      `EDate` DateTime,
      `IsInVehPos` tinyint(1) DEFAULT '0'
    );
  """

  @staticmethod
  def buildTupleFromProtobuf(objKey):
    """Builds a database tuple for a Protobuf object in S3

    Args:
      objKey: key of the Protobuf object
    """

    s3Mgr = s3.S3Mgr()
    data = s3Mgr.fetch_object_body(objKey)

    dts = []
    gtfsrt.process_entities(data,
        eachVehiclePos=lambda x:
            dts.append(datetime.utcfromtimestamp(x.timestamp))
    )
    kdt = s3.S3FeedKeyDT(objKey)
    mn = min(dts, default=None)
    mx = max(dts, default=None)
    return (objKey, len(dts), kdt, mn, mx)


  @staticmethod
  def selectProtobufKeysNotInVehPos(conn):
    """Retrieves S3 keys for Protobufs not yet in the VehPos table

    Args:
      conn: a DBConn instance
    """

    sqlStmt = """
      SELECT S3Key FROM `VehPosPb`
      WHERE NumRecs > 0 and not IsInVehPos;
    """

    ret = []
    cur = conn.execute(sqlStmt)
    for tpl in cur:
      ret.append(tpl[0])
    return ret


  @staticmethod
  def updateInVehPos(conn, objKey):
    """Marks an S3 Protobuf key as processed into the VehPos table

    Args:
      conn: a DBConn instance
      objKey: key for the Protobuf S3 object
    """

    sqlStmtMsk = """
      UPDATE `VehPosPb` SET `IsInVehPos` = True
      WHERE S3Key = %s;
    """
    conn.execute(sqlStmtMsk, objKey)


  @staticmethod
  def insertTpl(conn, tpl):
    """Inserts a record into the table

    Args:
      conn: a DBConn instance
      tpl: a tuple to insert
    """

    sqlStmt = """
      INSERT IGNORE INTO `VehPosPb` (
        S3Key, NumRecs, S3KeyDT, SDate, EDate
      )
      VALUES (%s, %s, %s, %s, %s);
    """
    conn.execute(sqlStmt, tpl)


class VehPos:
  """VehPos table helper class
  """

  TABLE_NAME = "VehPos"
  CREATE_STMT = """
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
  """

  @staticmethod
  def buildDBTuplesFromProtobuf(objKey):
    """Retrieves vehicle position tuples from a Protobuf file

    Timestamps are converted to naive UTC datetimes. Use this function to
    upload records to the database

    Args:
      objKey: Protobuf S3 key
    """

    def _vehpos_pb2_to_dbtpl_dtutc(pbVal):
      dt = datetime.utcfromtimestamp(pbVal.timestamp)
      return (
        pbVal.trip.route_id, dt, pbVal.vehicle.id, pbVal.trip.trip_id,
        pbVal.position.latitude, pbVal.position.longitude,
        pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
      )

    ret = []
    s3Mgr = s3.S3Mgr()
    data = s3Mgr.fetch_object_body(objKey)
    gtfsrt.process_entities(data,
        eachVehiclePos=lambda x: ret.append(
            VehPos._vehpos_pb2_to_dbtpl_dtutc(x)
        )
    )
    return ret


  # use this to create a pyspark Dataframe
  @staticmethod
  def buildDFTuplesFromProtobuf(objKey):
    """Retrieves vehicle position tuples from a Protobuf file

    Timestamps are converted to naive local datetimes. Use this function to
    write records to a Parquet file

    Args:
      objKey: Protobuf S3 key
    """

    def _vehpos_pb2_to_dbtpl_dtlocal(pbVal):
      dt = datetime.fromtimestamp(pbVal.timestamp)
      return (
        pbVal.trip.route_id, dt, pbVal.vehicle.id, pbVal.trip.trip_id,
        pbVal.position.latitude, pbVal.position.longitude,
        pbVal.current_status, pbVal.current_stop_sequence, pbVal.stop_id
      )

    ret = []
    s3Mgr = s3.S3Mgr()
    data = s3Mgr.fetch_object_body(objKey)
    gtfsrt.process_entities(data,
        eachVehiclePos=lambda x: ret.append(
            VehPos._vehpos_pb2_to_dbtpl_dtlocal(x)
        )
    )
    return ret

  @staticmethod
  def insertTpls(conn, tpls):
    """Inserts multiple records into the table through an executemany call

    Args:
      conn: a DBConn instance
      tpls: records to insert
    """

    sqlStmt = """
      INSERT IGNORE INTO `VehPos` (
        RouteId, DT, VehicleId, TripId, Lat, Lon, Status, StopSeq, StopId
      )
      VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    conn.executemany(sqlStmt, tpls)
