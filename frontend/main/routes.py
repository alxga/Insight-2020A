# pylint: disable=unused-import
# pylint: disable=redefined-outer-name

from flask import current_app
from flask import render_template, flash, redirect, request, url_for, jsonify

from common import Settings
from common.queryutils import DBConnCommonQueries

from . import bp


@bp.before_app_request
def before_request():
  pass


@bp.route('/', methods=['GET'])
@bp.route('/index', methods=['GET'])
def index():
  noteVP = "We have approximately %d vehicle positions so far"
  with DBConnCommonQueries() as con:
    noteVP = noteVP % con.count_approx("VehPos")
  return render_template('index.html', title=Settings.AppName,
                         note=noteVP)
