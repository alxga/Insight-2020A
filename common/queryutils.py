"""Helpers to connect to the database and run common queries"""

import mysql.connector

from .credentials import MySQLConnArgs
from .queries import Queries

__author__ = "Alex Ganin"


class DBConn:
  """Wrapper around mysql.connector supporting the 'with' pattern

  A 'with' enter call opens a connection and a cursor, which are closed on
  exit from the 'with' statement
  """

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
    """Executes a single SQL statement using the cursor

    Must be called within a 'with' statement

    Args:
      stmt: SQL query to execute
      params: optional parameters to pass to the query
    """

    self._cur.execute(stmt, params)
    self.uncommited += 1
    return self._cur


  def executemany(self, stmt, params):
    """Executes a SQL statement using the cursor for multiple sets of
    parameters

    Must be called within a 'with' statement

    Args:
      stmt: SQL query to execute
      params: an iterable of tuples to pass to the query
    """
    self._cur.executemany(stmt, params)
    self.uncommited += 1


  def commit(self):
    """Calls the commit on the connection
    """

    self._cnx.commit()
    self.uncommited = 0


class DBConnCommonQueries(DBConn):
  """Extension of DBConn class

  Simplifies some frequently used queries
  """

  def table_exists(self, tableName):
    """Checks whether a table exists

    Args:
      tableName: table name whose existence to check
    """

    sqlStmt = """
      SELECT COUNT(*)
      FROM information_schema.tables 
      WHERE table_schema = '%s' 
      AND table_name = '%s';
    """ % (MySQLConnArgs["database"], tableName)
    return next(self.execute(sqlStmt))[0] > 0


  def drop_table(self, tableName):
    """Drops a table from the database

    Args:
      tableName: name of the table to drop
    """

    sqlStmt = """
      DROP TABLE IF EXISTS `%s`;
    """ % (tableName)
    self.execute(sqlStmt)
    self.commit()


  def create_table(self, tableName, rewriteIfExists):
    """Creates a table in the database

    Args:
      tableName: name of the table to create, the schema must be defined
        in a query in the Queries dictionary with the key of the form
        "create" + tableName
      rewriteIfExists: if True and the table already exists it will be dropped
        and created again, if False and the table already exists no action will
        be taken

    Returns:
      True if a table was created, False if it already existed and was unchanged
    """

    createSqlStmt = Queries["create" + tableName]
    if self.table_exists(tableName):
      if rewriteIfExists:
        self.drop_table(tableName)
      else:
        return False
    self.execute(createSqlStmt)
    self.commit()
    return True


  def count_approx(self, tableName):
    """Returns an approximate number of records in a table

    Args:
      tableName: name of the table whose number of records to return
    """
    sqlStmt = Queries["countApprox"] % tableName
    cur = self.execute(sqlStmt)
    return next(cur)[0]


  def count_where(self, tableName, sqlWhere="True"):
    """Returns the exact number of records in a table

    Args:
      tableName: name of the table whose number of records to return
      sqlWhere: an optional WHERE expression to filter the records
    """
    sqlStmt = Queries["countApprox"] % (tableName, sqlWhere)
    cur = self.execute(sqlStmt)
    return next(cur)[0]
