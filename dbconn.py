import os
import mysql.connector

class DBConn:
  def __init__(self):
    self.cnx = None

  def connect(self):
    try:
      uName = os.environ['MYSQL_USER']
      pwd = os.environ['MYSQL_PWD']
      hst = os.environ['MYSQL_HOST']
      dbName = os.environ['MYSQL_DBNAME']
    except KeyError:
      return False

    self.cnx = mysql.connector.connect(user=uName, password=pwd,
                                       host=hst, database=dbName)
    return True

  def close(self):
    self.cnx.close()

dbConn = DBConn()
