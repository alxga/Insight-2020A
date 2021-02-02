import re
import decimal
from datetime import date

import pandas as pd
import boto3.dynamodb.types as dyndbtypes
from boto3.dynamodb.conditions import Key

from common import utils
from common import s3, dyndb, dbtables, Settings
from common.queryutils import DBConn
from common.dyndb import DynDBMgr

__author__ = "Alex Ganin"

logger = utils.get_logger()


def _process_df(df, pqDate):
  dynMgr = dyndb.DynDBMgr()
  dynTbl = dynMgr.table('hlydelays')
  with dynTbl.batch_writer() as dynWriter, \
       decimal.localcontext(dyndbtypes.DYNAMODB_CONTEXT) as decCtx:
    decCtx.traps[decimal.Inexact] = False
    decCtx.traps[decimal.Rounded] = False
    for row in df.itertuples(index=False):
      if row.route_stop[0:3] != 'Red':
        continue
      vals = [{
        'HourEST': x['HourEST'],
        'AvgDelay': decCtx.create_decimal_from_float(x['AvgDelay']),
        'AvgDist': decCtx.create_decimal_from_float(x['AvgDist']),
        'Cnt': int(x['Cnt'])
      } for x in row.vals]
      d = {
        'route_stop': row.route_stop,
        'date': pqDate.strftime('%Y%m%d'),
        'vals': vals
      }
      dynWriter.put_item(Item=d)



def _process_pqdate(pqDate):
  s3_prefix = f"HlyDelays/{pqDate.strftime('%Y%m%d')}.pq"
  rexpr = re.compile(r'.*part-.*\.parquet$')
  s3Mgr = s3.S3Mgr()
  for key in s3Mgr.fetch_keys(s3_prefix):
    if rexpr.match(key):
      key = f's3://{Settings.S3BucketName}/{key}'
      df = pd.read_parquet(key)
      _process_df(df, pqDate)
      logger.info(f'Processed entries from {key}')


def main():
  """Checks for feed updates on the MBTA website and saves any updates to S3
  """

  with DBConn() as conn:
    wh_stmt = 'IsInHlyDelaysS3 AND NOT IsInHlyDelaysDyn'
    entriesToProcess = dbtables.PqDates \
      .select_pqdates_not_in_delays(conn, wh_stmt)
  for targetDate in entriesToProcess:
    _process_pqdate(targetDate)
    with DBConn() as conn:
      dbtables.PqDates.update_in_delays(conn, targetDate, "IsInHlyDelaysDyn")
      conn.commit()


def main_dbg():
  routeId = 'Red'
  stopName = 'Harvard'
  dynKey = f'{routeId}:::[{stopName}]'
  dynDb = DynDBMgr()
  dynTbl = dynDb.table('hlydelays')
  response = dynTbl.query(
    KeyConditionExpression=Key('route_stop').eq(dynKey)
  )
  for item in response['Items']:
    
    dt = datetime(rec.DateEST.year, rec.DateEST.month, rec.DateEST.day,
                  rec.HourEST, 30, 0)
    dt = Settings.MBTA_TZ.localize(dt)


if __name__ == "__main__":
  main_dbg()
