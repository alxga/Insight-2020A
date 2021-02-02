from io import StringIO
from collections import namedtuple
import csv
from flask import jsonify, request, Response

from boto3.dynamodb.conditions import Key

from common.queryutils import DBConn
from common.dyndb import DynDBMgr
from .. import math
from . import bp

__author__ = "Alex Ganin"


@bp.route('/routeids', methods=['GET'])
def get_routeids():
  sqlStmt = """
    SELECT DISTINCT RouteId FROM RouteStops
    WHERE RouteId <> 'ALLROUTES' AND RouteId <> 'ALLTRAINS' AND
      RouteId <> 'ALLBUSES'
    ORDER BY 1;
  """
  ret = []
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      if row[0]:
        ret.append(row[0])
  data = {
    "items": ret
  }
  return jsonify(data)


@bp.route('/stopnames', methods=['GET'])
def get_stopnames():
  routeId = request.args.get('routeId', None)
  q = request.args.get('q', '').lower()

  sqlStmt = """
    SELECT DISTINCT StopName FROM RouteStops
  """
  if routeId:
    sqlStmt += " WHERE RouteId = %s"
    params = (routeId,)
  else:
    params = None
  sqlStmt += " ORDER BY 1;"

  ret = []
  with DBConn() as con:
    cur = con.execute(sqlStmt, params)
    for row in cur:
      if not row[0] or row[0] == 'ALLSTOPS':
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
  if not routeId:
    routeId = 'ALLROUTES'
  if not stopName:
    stopName = 'ALLSTOPS'
  dynKey = f'{routeId}:::[{stopName}]'
  dynDb = DynDBMgr()
  dynTbl = dynDb.table('hlydelays')
  response = dynTbl.query(
    KeyConditionExpression=Key('route_stop').eq(dynKey)
  )

  Record = namedtuple("HlyDelayRec", "DT_EST AvgDelay Cnt")
  recs = []
  for it in sorted(response['Items'], key=lambda x: x['date']):
    for val in it['vals']:
      # dt = datetime.strptime(val['DT_EST'], '%Y-%m-%d %H-%M-%S')
      # dt = Settings.MBTA_TZ.localize(dt)
      recs.append(Record(
        val['DT_EST'], float(val['AvgDelay']), float(val['Cnt'])
      ))
  if len(recs) < 2:
    return []

  x = [rec.AvgDelay for rec in recs]
  w = [rec.Cnt for rec in recs]
  xsmoothed = math.rolling_weighted_triangle_conv(x, w, 7)

  data = []
  for i, rec in enumerate(recs):
    rec = recs[i]
    data.append({
      "dt": rec.DT_EST,
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
