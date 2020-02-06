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

  def count_approx(self, tableName):
    sqlStmt = Queries["countApprox"] % tableName
    cur = self.execute(sqlStmt)
    return next(cur)[0]

  def count_where(self, tableName, sqlWhere="True"):
    sqlStmt = Queries["countApprox"] % (tableName, sqlWhere)
    cur = self.execute(sqlStmt)
    return next(cur)[0]
