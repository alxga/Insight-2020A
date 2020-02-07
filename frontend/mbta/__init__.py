# pylint: disable=wrong-import-position
# pylint: disable=invalid-name

from flask import Blueprint

bp = Blueprint('mbta', __name__)

from . import routes
