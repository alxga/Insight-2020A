from common.credentials import MySQLConnArgs

class Config:
  SQLALCHEMY_DATABASE_URI = \
    "mysql+pymysql://{user}:{password}@{host}/{database}" \
    .format(**MySQLConnArgs)
  SQLALCHEMY_TRACK_MODIFICATIONS = False
