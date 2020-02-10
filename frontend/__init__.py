# pylint: disable=wrong-import-position
# pylint: disable=import-outside-toplevel
# pylint: disable=invalid-name
# pylint: disable=usused-variable

import os
from flask import Flask, request, current_app, url_for, redirect
from flask_bootstrap import Bootstrap

__author__ = "Alex Ganin"


bootstrap = Bootstrap()

def create_app():
  app = Flask(__name__)

  bootstrap.init_app(app)

  from .mbta import bp as mbta_bp
  app.register_blueprint(mbta_bp, url_prefix="/mbta")
  from .api import bp as api_bp
  app.register_blueprint(api_bp, url_prefix="/mbta/api")

  @app.route('/', methods=['GET'])
  @app.route('/index', methods=['GET'])
  def index():
    return redirect(url_for('mbta.index'), code=302)

  return app
