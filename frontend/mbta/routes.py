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


@bp.route('/slides', methods=['GET'])
def slides():
  slides_url = "https://docs.google.com/presentation/d/1AwABJ4udwEj1ofg99GbL5I2vhLG6S0Bxh877UUzRBKE/edit?usp=sharing"
  return redirect(slides_url, code=302)
