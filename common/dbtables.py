"""Database tables helper classes"""

from datetime import datetime

from . import s3
from . import gtfsrt

def create_if_not_exists(conn, cls):
  """Creates a table if it doesn't exist

  Args:
    conn: a DBConnCommonQueries instance
    cls: table class, e.g., VehPos
  """

  if not conn.table_exists(cls.TABLE_NAME):
    if 'CREATE_STMT' in cls.__dict__:
      conn.execute(cls.CREATE_STMT)
    if 'CREATE_STMTS' in cls.__dict__:
      for stmt in cls.CREATE_STMTS:
        conn.execute(stmt)
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
  def insert_values(conn, prefix, numRecs):
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
  def select_prefixes_dict(conn):
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
  def build_tuple_from_protobuf(objKey):
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
  def select_protobuf_keys_not_invehpos(conn):
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
  def select_protobuf_keys_between_dates(conn, dt1, dt2):
    """Retrieves S3 keys for Protobufs downloaded between two datetimes

    Args:
      conn: a DBConn instance
      dt1, dt2: datetime between which the returned Protobufs were downloaded
    """

    sqlStmt = """
      SELECT S3Key FROM `VehPosPb`
      WHERE NumRecs > 0 and S3KeyDT > %s and S3KeyDT < %s;
      """
    cur = conn.execute(sqlStmt, (dt1, dt2))
    ret = []
    for tpl in cur:
      ret.append(tpl[0])
    return ret


  @staticmethod
  def update_invehpos(conn, objKey):
    """Marks an S3 Protobuf key as processed into the VehPos table

    Args:
      conn: a DBConn instance
      objKey: key for the Protobuf S3 object
    """

    sqlStmtMsk = """
      UPDATE `VehPosPb` SET `IsInVehPos` = True
      WHERE S3Key = %s;
    """
    conn.execute(sqlStmtMsk, (objKey,))


  @staticmethod
  def insert_tpl(conn, tpl):
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
  def build_db_tuples_from_pb(objKey):
    """Retrieves vehicle position tuples from a Protobuf file

    Timestamps are converted to naive UTC datetimes. Use this function to
    upload records to the database.

    Args:
      objKey: Protobuf S3 key
    """

    def vehpos_pb2_to_dbtpl_dtutc(pbVal):
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
        eachVehiclePos=lambda x: ret.append(vehpos_pb2_to_dbtpl_dtutc(x))
    )
    return ret


  # use this to create a pyspark Dataframe
  @staticmethod
  def build_df_tuples_from_pb(objKey):
    """Retrieves vehicle position tuples from a Protobuf file

    Timestamps are converted to naive local datetimes. Use this function to
    write records to a Parquet file.

    Args:
      objKey: Protobuf S3 key
    """

    def vehpos_pb2_to_dbtpl_dtlocal(pbVal):
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
        eachVehiclePos=lambda x: ret.append(vehpos_pb2_to_dbtpl_dtlocal(x))
    )
    return ret

  @staticmethod
  def insert_tpls(conn, tpls):
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


class PqDates:
  """PqDates table helper class
  """

  TABLE_NAME = "PqDates"
  CREATE_STMT = """
    CREATE TABLE `PqDates` (
      `D` Date PRIMARY KEY,
      `NumKeys` integer NOT NULL,
      `NumRecs` integer NOT NULL,
      `IsInHlyDelays` tinyint(1) DEFAULT '0'
    );
  """

  @staticmethod
  def select_existing_pqdates(conn):
    """Returns a dictionary of all Parquet dates from the database table

    Args:
      conn: a DBConn instance
    """

    sqlStmt = """
      SELECT D FROM `PqDates`;
    """
    ret = {}
    cur = conn.execute(sqlStmt)
    for row in cur:
      ret[row[0]] = 1
    return ret


  @staticmethod
  def select_pqdates_not_in_delays(conn):
    """Retrieves a list of Parquet files for which delays need to be calculated

    Args:
      conn: a DBConn instance
    """

    sqlStmt = """
      SELECT D, IsInHlyDelays FROM `PqDates`
      WHERE NumRecs > 0 and not IsInHlyDelays;
    """
    ret = []
    cur = conn.execute(sqlStmt)
    for row in cur:
      ret.append({
        "Date": row[0],
        "IsInHlyDelays": row[1]
      })
    return ret

  @staticmethod
  def select_latest_processed(conn):
    """Returns the latest date for which delays were calculated from Parquet

    Args:
      conn: a DBConn instance
    """

    sqlStmt = """
      SELECT max(D) FROM `PqDates`
      WHERE NumRecs > 0 and IsInHlyDelays;
    """
    cur = conn.execute(sqlStmt)
    try:
      row = next(cur)
      return row[0]
    except StopIteration:
      return None


  @staticmethod
  def update_in_delays(conn, D, delaysColName):
    """Sets a delays column in a PqDates table to True

    Args:
      conn: a DBConn instance
      D: Parquet file date for which the column should be set
      delaysColName: column name - IsInHlyDelays
    """

    sqlStmt = """
      UPDATE `PqDates` SET `%s` = True
      WHERE D = '%s';
    """ % (delaysColName, D.strftime("%Y-%m-%d"))
    conn.execute(sqlStmt)


  @staticmethod
  def insert_values(conn, name, numKeys, numRecs):
    """Inserts a record describing a newly created Parquet file into the table

    Args:
      conn: a DBConn instance
      name: Parquet file date
      numKeys: number of Protobuf files processed into that Parquet file
      numRecs: number of vehicle position records in the Parquet file
    """

    sqlStmt = """
      INSERT INTO `PqDates` (
        D, NumKeys, NumRecs
      )
      VALUES (%s, %s, %s);
    """
    conn.execute(sqlStmt, (name, numKeys, numRecs))


class VPDelays:
  """VPDelays table helper class
  """

  TABLE_NAME = "VPDelays"
  CREATE_STMT = """
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
  """

  @staticmethod
  def insert_row(conn, row, pqDate):
    """Inserts a dataframe row into the table

    Args:
      conn: a DBConn instance
      row: a dataframe row to insert
      pqDate: value for the `D` column of the table (Parquet file date)
    """

    sqlStmt = """
      INSERT INTO `VPDelays` (
        D, RouteId, TripId, StopId, StopName, StopLat, StopLon, SchedDT,
        EstLat, EstLon, EstDT, EstDist, EstDelay
      )
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    tpl = (
      pqDate, row.RouteId, row.TripId, row.StopId, row.StopName,
      row.StopLat, row.StopLon, row.SchedDT, row.EstLat, row.EstLon,
      row.EstDT, row.EstDist, row.EstDelay
    )
    conn.execute(sqlStmt, tpl)


  @staticmethod
  def delete_for_parquet(conn, D):
    """Removes records for a particular Parquet file from the table

    Args:
      conn: a DBConn instance
      D: Parquet file date as saved in the table column `D`
    """

    sqlStmt = """
      DELETE FROM `VPDelays` WHERE D = '%s';
    """ % D.strftime("%Y-%m-%d")
    conn.execute(sqlStmt)


class HlyDelays:
  """HlyDelays table helper class
  """

  TABLE_NAME = "HlyDelays"
  CREATE_STMT = """
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
  """

  @staticmethod
  def insert_row(conn, row, pqDate, noRouteVal):
    """Inserts a dataframe row into the table

    Args:
      conn: a DBConn instance
      row: a dataframe row to insert
      pqDate: value for the `D` column of the table (Parquet file date)
      noRouteVal: value to use when row has no RouteId, used for aggregations
        for all buses or all trains when it's set to "ALLBUSES" or "ALLTRAINS"
        instead of NULL
    """

    sqlStmt = """
      INSERT INTO `HlyDelays` (
        D, DateEST, HourEST, RouteId, StopName, AvgDelay, AvgDist, Cnt,
        StopLat, StopLon, StopId
      )
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    tpl = (
      pqDate,
      row.DateEST, row.HourEST,
      getattr(row, "RouteId", noRouteVal),
      getattr(row, "StopName", None),
      row.AvgDelay, row.AvgDist, row.Cnt,
      getattr(row, "StopLat", None),
      getattr(row, "StopLon", None),
      getattr(row, "StopId", None)
    )
    conn.execute(sqlStmt, tpl)


  @staticmethod
  def delete_for_parquet(conn, D):
    """Removes records for a particular Parquet file from the table

    Args:
      conn: a DBConn instance
      D: Parquet file date as saved in the table column `D`
    """

    sqlStmt = """
      DELETE FROM `HlyDelays` WHERE D = '%s';
    """ % D.strftime("%Y-%m-%d")
    conn.execute(sqlStmt)


class RouteStops:
  """DynKeys table helper class
  """

  TABLE_NAME = "RouteStops"
  CREATE_STMTS = [
    """
      CREATE TABLE `RouteStops` (
        `RouteId` char(50) NOT NULL,
        `StopName` char(200) NOT NULL,
        PRIMARY KEY(`RouteId`, `StopName`)
      );
    """,
    "CREATE INDEX ixRouteId ON `RouteStops`(`RouteId`);",
    "CREATE INDEX ixStopName ON `RouteStops`(`StopName`);"
  ]

  @staticmethod
  def insert_values(conn, rows):
    conn.execute("DROP TABLE IF EXISTS temp_RouteStops;")
    sql = """
      CREATE TEMPORARY TABLE temp_RouteStops(
        RouteId VARCHAR(500),
        StopName VARCHAR(500)
      );
    """
    conn.execute(sql)
    for r in rows:
      sql = "INSERT INTO temp_RouteStops VALUES(%s, %s);"
      conn.execute(sql, tuple(x for x in r))

    sql = """
      INSERT INTO RouteStops
      SELECT UPD.*
      FROM temp_RouteStops UPD
        LEFT JOIN RouteStops T
          ON UPD.RouteId = T.RouteId AND UPD.StopName = T.StopName
      WHERE T.RouteId IS NULL;
    """
    conn.execute(sql)
