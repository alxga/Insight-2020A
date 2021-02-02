from io import StringIO
from datetime import datetime
from collections import namedtuple
import csv
from flask import jsonify, request, Response
from common.queryutils import DBConn
from common.dyndb import DynDBMgr
from common import Settings
from .. import math
from . import bp

__author__ = "Alex Ganin"


@bp.route('/routeids', methods=['GET'])
def get_routeids():
  sqlStmt = """
    SELECT DISTINCT RouteId FROM RouteStops ORDER BY 1;
  """
  ret = []
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      if row[0] and row[0] != 'ALLBUSES' and row[0] != 'ALLTRAINS' and \
         row[0] != 'ALLROUTES':
        ret.append(row[0])
  data = {
    "items": ret
  }
  return jsonify(data)


@bp.route('/stopnames', methods=['GET'])
def get_stopnames():
  routeId = request.args.get('routeId', None)
  q = request.args.get('q', '').lower()

  sqlStmt = "SELECT DISTINCT StopName FROM RouteStops"
  if routeId:
    sqlStmt += " WHERE RouteId = %s"
    params = (routeId,)
  else:
    sqlStmt += " WHERE RouteId IS NULL"
    params = None
  sqlStmt += " ORDER BY 1;"

  ret = []
  with DBConn() as con:
    cur = con.execute(sqlStmt, params)
    for row in cur:
      if not row[0]:
        continue
      stopName = row[0].strip()
      stopNameLower = stopName.lower()
      if stopNameLower.startswith(q):
        ret.append(stopName)
  data = {
    "items": ret
  }
  return jsonify(data)


def query_delays_hourly(routeId, stopName):
  params = []
  dynKey = f'{routeId}:::[{stopName}]'
  dynDb = DynDBMgr()
  dynTbl = dynDb.table('hlydelays')
  response = dynTbl.query(
    KeyConditionExpression=Key('route_stop').eq(dynKey)
  )
  for item in response['Items']:
    pass

  Record = namedtuple("HlyDelayRec", "DateEST HourEST AvgDelay AvgDist Cnt")
  with DBConn() as con:
    cur = con.execute(sqlStmt, params)
    records = [Record(*row) for row in cur]

  data = []

  if len(records) > 2:
    x = [rec.AvgDelay for rec in records]
    w = [rec.Cnt for rec in records]
    xsmoothed = math.rolling_weighted_triangle_conv(x, w, 7)

    for i, rec in enumerate(records):
      rec = records[i]
      dt = datetime(rec.DateEST.year, rec.DateEST.month, rec.DateEST.day,
                    rec.HourEST, 30, 0)
      dt = Settings.MBTA_TZ.localize(dt)
      data.append({
        "dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "value": xsmoothed[i],
        "cnt": rec.Cnt,
        "unweighted_value": x[i]
      })

  return data


def write_delays_hourly_csv(ioObj, data):
  writer = csv.writer(ioObj, delimiter=',')
  columns = ["dt", "value", "cnt", "unweighted_value"]
  writer.writerow(columns)
  for rec in data:
    row = []
    for col in columns:
      row.append(rec[col])
    writer.writerow(row)


@bp.route('/delays-hourly', methods=['GET'])
def get_delays_hourly():
  routeId = request.args.get('routeId', None)
  stopName = request.args.get('stopName', None)

  data = query_delays_hourly(routeId, stopName)

  if request.accept_mimetypes["text/csv"] > \
     request.accept_mimetypes["application/json"]:
    resp = Response()
    resp.headers["content_type"] = "text/csv"
    ioObj = StringIO("")
    write_delays_hourly_csv(ioObj, data)
    resp.set_data(ioObj.getvalue())
    ioObj.close()
    return resp
  else:
    return jsonify(data)
