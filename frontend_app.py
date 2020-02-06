# pylint: disable=invalid-name

from frontend import create_app
from common.queryutils import DBConn

app = create_app()

@app.shell_context_processor
def make_shell_context():
  return {'DBConn': DBConn}
