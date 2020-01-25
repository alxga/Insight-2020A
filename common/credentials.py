import os

S3ConnArgs = {
  "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
  "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
  "region_name": os.environ["AWS_DEFAULT_REGION"]
}

MySQLConnArgs = {
  "user": os.environ['MYSQL_USER'],
  "password": os.environ['MYSQL_PWD'],
  "host": os.environ['MYSQL_HOST'],
  "database": os.environ['MYSQL_DBNAME']
}
