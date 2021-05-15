# pylint: disable=wrong-import-position
# pylint: disable=import-outside-toplevel
# pylint: disable=invalid-name
# pylint: disable=usused-variable

from flask import Flask, url_for, redirect
from flask_bootstrap import Bootstrap
from common import Settings
from .staticdata import init_static_data

__author__ = "Alex Ganin"


bootstrap = Bootstrap()


def create_app():
  app = Flask(__name__)

  bootstrap.init_app(app)

  if Settings.StaticDataPath:
    init_static_data(Settings.StaticDataPath)

  from .mbta import bp as mbta_bp
  app.register_blueprint(mbta_bp, url_prefix="/mbta")
  from .api import bp as api_bp
  app.register_blueprint(api_bp, url_prefix="/mbta/api")

  @app.route('/', methods=['GET'])
  @app.route('/index', methods=['GET'])
  def index():
    return redirect(url_for('mbta.index'), code=302)

  return app
