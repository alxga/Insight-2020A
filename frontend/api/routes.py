# pylint: disable=unused-import

from datetime import datetime
from collections import namedtuple
import pytz
from flask import jsonify, request, url_for, g, abort
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


@bp.route('/delays-hourly', methods=['GET'])
def get_delays_hourly():
  routeId = request.args.get('routeId', None)
  stopName = request.args.get('stopName', None)
  dayOfWeek = request.args.get('dayOfWeek', None)

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

  if len(records) > 2:
    x = [rec.AvgDelay for rec in records]
    w = [rec.Cnt for rec in records]
    xsmoothed = math.rolling_weighted_triangle_conv(x, w, 7)

    for i, rec in enumerate(records):
      dt = datetime(rec.DateEST.year, rec.DateEST.month, rec.DateEST.day,
                    rec.HourEST, 30, 0) \
        .replace(tzinfo=pytz.timezone("EST"))
      data.append({
        "dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "value": xsmoothed[i]
      })

  return jsonify(data)
