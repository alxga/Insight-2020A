# pylint: disable=wrong-import-position
# pylint: disable=invalid-name

__author__ = "Alex Ganin"


from flask import Blueprint

bp = Blueprint('api', __name__)

from . import routes
