"""Frontend Flask app definition script
"""

# pylint: disable=invalid-name

from frontend import create_app

app = create_app()

@app.shell_context_processor
def make_shell_context():
  from common.queryutils import DBConn
  return {'DBConn': DBConn}
