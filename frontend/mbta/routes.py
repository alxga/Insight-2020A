from flask import render_template

from common import Settings

from . import bp

__author__ = "Alex Ganin"


@bp.route('/', methods=['GET'])
@bp.route('/index', methods=['GET'])
def index():
  return render_template(
    'index.html', appName=Settings.AppName, footerLine=Settings.FooterLine
  )
