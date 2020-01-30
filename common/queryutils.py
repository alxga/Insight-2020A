from datetime import datetime

import mysql.connector

from .credentials import MySQLConnArgs
from .queries import Queries

__author__ = "Alex Ganin"


class DBConn:
  def __init__(self):
    self._cnx = None
    self._cur = None
    self.uncommited = 0

  def __enter__(self):
    self._cnx = mysql.connector.connect(**MySQLConnArgs)
    self._cur = self._cnx.cursor()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._cur:
      self._cur.close()
    if self._cnx:
      self._cnx.close()

  def execute(self, stmt, params=None):
    self._cur.execute(stmt, params)
    self.uncommited += 1
    return self._cur

  def executemany(self, stmt, params):
    self._cur.executemany(stmt, params)
    self.uncommited += 1

  def commit(self):
    self._cnx.commit()
    self.uncommited = 0


class DBConnCommonQueries(DBConn):
  def fetch_dates_to_update(self, whereStmt="True"):
    sqlStmt = """
      SELECT DISTINCT Date(S3KeyDT) FROM VehPosPb WHERE %s;
    """ % (whereStmt)
    dtUtcNow = datetime.utcnow()

    ret = []
    cursor = self.execute(sqlStmt)
    for tpl in cursor:
      dt = tpl[0]
      # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
      if dtUtcNow > datetime(dt.year, dt.month, dt.day + 1, 8):
        ret.append(dt)
    return ret

  def table_exists(self, tableName):
    sqlStmt = """
      SELECT COUNT(*)
      FROM information_schema.tables 
      WHERE table_schema = '%s' 
      AND table_name = '%s';
    """ % (MySQLConnArgs["database"], tableName)
    return next(self.execute(sqlStmt))[0] > 0

  def drop_table(self, tableName):
    sqlStmt = """
      DROP TABLE IF EXISTS `%s`;
    """ % (tableName)
    self.execute(sqlStmt)
    self.commit()

  def create_table(self, tableName, rewriteIfExists):
    createSqlStmt = Queries["create" + tableName]
    if self.table_exists(tableName):
      if rewriteIfExists:
        self.drop_table(tableName)
      else:
        return False
    self.execute(createSqlStmt)
    self.commit()
