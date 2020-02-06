# pylint: disable=wrong-import-position
# pylint: disable=import-outside-toplevel
# pylint: disable=invalid-name

import os
from flask import Flask, request, current_app
from flask_bootstrap import Bootstrap
from frontend_config import Config


bootstrap = Bootstrap()

def create_app(config_class=Config):
  app = Flask(__name__)
  app.config.from_object(config_class)

  bootstrap.init_app(app)

  from .main import bp as main_bp
  app.register_blueprint(main_bp)

  return app
