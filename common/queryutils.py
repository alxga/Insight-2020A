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
