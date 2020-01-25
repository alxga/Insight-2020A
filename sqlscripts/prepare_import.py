import os
import sys


if __name__ != "__main__":
  print("%s cannot be imported" % __file__)
  exit(1)

if len(sys.argv) < 2:
  print("Usage python3 %s <OUTPUT PATH>" % (__file__))
  exit(1)

_PyPath = os.path.abspath(os.path.dirname(__file__))
_OutPath = sys.argv[1]

_OutFile = open(_OutPath, "wt")

_OutFile.write('Queries = {\n')

try:
  for entry in os.listdir(_PyPath):
    if not os.path.isfile(entry) or \
       len(entry) < 5 or entry[-4:].lower() != '.sql':
      continue

    varName = os.path.splitext(entry)[0]
    with open(os.path.join(_PyPath, entry), "rt") as f:
      contents = f.read()

    _OutFile.write('"%s": """' % varName)
    _OutFile.write('%s\n' % contents)
    _OutFile.write('""",\n')

  _OutFile.write('}')

finally:
  _OutFile.close()

