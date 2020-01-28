from datetime import datetime

import mysql.connector

from .credentials import MySQLConnArgs

__author__ = "Alex Ganin"


def set_vehpospb_flag(flagName, value, objKeys):
  cnx = None
  cursor = None
  sqlStmtMsk = """
    UPDATE VehPosPb SET `%s` = %s WHERE S3Key = '%s';
  """

  try:
    cnx = mysql.connector.connect(**MySQLConnArgs)
    cursor = cnx.cursor()
    uncommited = 0
    for objKey in objKeys:
      cursor.execute(sqlStmtMsk % (flagName, value, objKey))
      print(sqlStmtMsk % (flagName, value, objKey))
      uncommited += 1
      if uncommited >= 100:
        cnx.commit()
        uncommited = 0
    if uncommited > 0:
      cnx.commit()
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()


def fetch_dates_to_update(whereStmt=None):
  cnx = None
  cursor = None
  if whereStmt is None:
    whereStmt = "True"
  sqlStmt = """
    SELECT DISTINCT Date(S3KeyDT) FROM VehPosPb WHERE %s;
  """ % (whereStmt)
  dtUtcNow = datetime.utcnow()

  ret = []
  try:
    cnx = mysql.connector.connect(**MySQLConnArgs)
    cursor = cnx.cursor()
    cursor.execute(sqlStmt)
    for tpl in cursor:
      dt = tpl[0]
      # we define new day to start at 8:00 UTC (3 or 4 at night Boston time)
      if dtUtcNow > datetime(dt.year, dt.month, dt.day + 1, 8):
        ret.append(dt)
    return ret
  finally:
    if cursor:
      cursor.close()
    if cnx:
      cnx.close()
