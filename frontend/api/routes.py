from io import StringIO
from datetime import datetime
from collections import namedtuple
import csv
import pytz
from flask import jsonify, request, Response
from common.queryutils import DBConn
from .. import math
from . import bp


@bp.route('/routeids', methods=['GET'])
def get_routeids():
  sqlStmt = """
    SELECT DISTINCT RouteId FROM HlyDelays ORDER BY 1;
  """
  ret = []
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      if row[0] and row[0] != 'ALLBUSES' and row[0] != 'ALLTRAINS':
        ret.append(row[0])
  data = {
    "items": ret
  }
  return jsonify(data)


@bp.route('/stopnames', methods=['GET'])
def get_stopnames():
  routeId = request.args.get('routeId', None)
  q = request.args.get('q', '').lower()

  sqlStmt = "SELECT DISTINCT StopName FROM HlyDelays"
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
  sqlStmt = """
    SELECT DateEST, HourEST, AvgDelay, AvgDist, Cnt FROM HlyDelays WHERE
  """
  if routeId:
    sqlStmt += " RouteId = %s and"
    params.append(routeId)
  else:
    sqlStmt += " RouteId IS NULL and"

  if stopName:
    sqlStmt += " StopName = %s"
    params.append(stopName)
  else:
    sqlStmt += " StopName IS NULL"
  sqlStmt += " ORDER BY DateEST, HourEST;"

  Record = namedtuple("HlyDelayRec", "DateEST HourEST AvgDelay AvgDist Cnt")
  with DBConn() as con:
    cur = con.execute(sqlStmt, params)
    records = [Record(*row) for row in cur]

  data = []

  if len(records) >= 2:
    x = [rec.AvgDelay for rec in records]
    w = [rec.Cnt for rec in records]
    xsmoothed = math.rolling_weighted_triangle_conv(x, w, 7)

    for i, rec in enumerate(records):
      dt = datetime(rec.DateEST.year, rec.DateEST.month, rec.DateEST.day,
                    rec.HourEST, 30, 0) \
        .replace(tzinfo=pytz.timezone("EST"))
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
