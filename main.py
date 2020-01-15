import os
from datetime import datetime
import requests

url = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
url = "http://www.pdf995.com/samples/pdf.pdf"
r = requests.get(url)
print("Retrieved %d bytes\n" % len(r.content))

DumpFName = datetime.now().strftime("%Y%m%d-%H%M%S.pb")
PyPath = os.path.dirname(os.path.abspath(__file__))
DumpDir = os.path.join(PyPath, "VP")
if not os.path.exists(DumpDir):
    os.makedirs(DumpDir)
DumpFilePath = os.path.join(DumpDir, DumpFName)

with open(DumpFilePath, "wb") as handle:
    handle.write(r.content)
