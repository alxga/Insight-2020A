import re
import decimal
from datetime import datetime

import pandas as pd
import boto3.dynamodb.types as dyndbtypes

from common import utils
from common import s3, dyndb, dbtables, Settings
from common.queryutils import DBConn

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
      # m = re.match(r'(?P<route>.*):::\[(?P<stop>.*)\]', row.route_stop)
      # if not m:
      #   continue
      # if m['route'] != 'Red' and (
      #   m['route'] != 'ALLROUTES' or (m['stop'] != 'Harvard' and m['stop'] != 'ALLSTOPS')
      # ):
      #   continue
      vals = []
      for x in row.vals:
        dt = x['DateEST']
        dt = datetime(dt.year, dt.month, dt.day, x['HourEST'], 30, 0)
        vals.append({
          'DT_EST': dt.strftime('%Y-%m-%d %H:%M:%S'),
          'AvgDelay': decCtx.create_decimal_from_float(x['AvgDelay']),
          'AvgDist': decCtx.create_decimal_from_float(x['AvgDist']),
          'Cnt': int(x['Cnt'])
        })
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


if __name__ == "__main__":
  main()
