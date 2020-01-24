import os

_PyPath = os.path.abspath(os.path.dirname(__file__))

def load(fName):
  if len(fName) < 5 or fName[-4:] .lower() != ".sql":
    fName = fName + ".sql"
  fullPath = os.path.join(_PyPath, fName)
  with open(fullPath, "rt") as f:
    return ''.join(f.readlines()) + ';'
