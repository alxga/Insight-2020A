from common import s3

s3Mgr = s3.S3Mgr()

for pfx in ("Alerts", "VehiclePos", "TripUpdates"):
  keys = s3Mgr.fetch_keys("pb/" + pfx, 100)
  for key in keys:
    if key[-10] == '-':
      nKey = key.replace('-', '/')
      s3Mgr.move_key(key, nKey)
