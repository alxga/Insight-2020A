# pylint: disable=unused-import

from flask import jsonify, request, url_for, g, abort
from common.queryutils import DBConn
from . import bp

@bp.route('/routeids', methods=['GET'])
def get_routeids():
  sqlStmt = """
    SELECT DISTINCT RouteId FROM VPDelays ORDER BY 1;
  """
  ret = []
  with DBConn() as con:
    cur = con.execute(sqlStmt)
    for row in cur:
      ret.append(row[0])
  data = {
    "items": ret
  }
  return jsonify(data)

@bp.route('/stopnames', methods=['GET'])
def get_stopnames():
  routeId = request.args.get('routeId', None)
  q = request.args.get('q', '').lower()

  sqlStmt = "SELECT DISTINCT StopName FROM VPDelays"
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
      stopName = row[0].strip()
      stopNameLower = stopName.lower()
      if stopNameLower.startswith(q):
        ret.append(stopName)
  data = {
    "items": ret
  }
  return jsonify(data)
