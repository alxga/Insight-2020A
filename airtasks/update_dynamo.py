import re
import decimal
from datetime import date

import pandas as pd
import boto3.dynamodb.types as dyndbtypes

from common import s3, dyndb, Settings
from common import utils

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

  tg_dates = [
    date(2020, 7, 10), date(2020, 7, 11), date(2020, 7, 12),
    date(2020, 7, 13), date(2020, 7, 14)
  ]
  for dt in tg_dates:
    _process_pqdate(dt)

if __name__ == "__main__":
  main()
