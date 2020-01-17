import os
import time
from datetime import datetime
import requests
import threading

Feeds = [
    ("VehiclePos", "https://cdn.mbta.com/realtime/VehiclePositions.pb", 1),
    ("TripUpdates", "https://cdn.mbta.com/realtime/TripUpdates.pb", 60),
    ("Alerts", "https://cdn.mbta.com/realtime/Alerts.pb", 30)
]

PyPath = os.path.dirname(os.path.abspath(__file__))
for tpl in Feeds:
    p = os.path.join(PyPath, tpl[0])
    if not os.path.exists(p):
        os.makedirs(p)


def download_feed(dir, url, *args):
    fName = datetime.now().strftime("%Y%m%d-%H%M%S.pb")
    r = requests.get(url)
    fPath = os.path.join(PyPath, dir, fName)
    with open(fPath, "wb") as handle:
        handle.write(r.content)


threads = []
for sec in range(0, 59, 5):
    print("Offset %d\n" % sec)
    for tpl in Feeds:
        if sec % tpl[2] != 0:
            continue
        t = threading.Thread(target=download_feed, args=tpl)
        t.start()
        threads.append(t)
    time.sleep(5)

for t in threads:
    t.join()
