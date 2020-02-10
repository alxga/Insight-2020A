The folder contains modified 3rd-party packages and files

Only **transitfeed/shapelib.py** and **gtfs-realtime.proto** are currently needed for the main app

## Packages and Files

Package or File | Role
---- | ----
**gtfsscheduleviewer/** | Scripts to display a Marey graph in **schedule_viewer.py**
**transitfeed/** | Library to parse and validate static GTFS feeds
**\_\_init__.py** | Converts this folder to a Python package to simplify imports
**gtfs-realtime.proto** | Main launch script
**schedule_viewer.py** | Stores and computes configuration settings and paths for the tool

## Notes

**gtfsscheduleviewer**, **transitfeed**, and **schedule_viewer.py** were downloaded from [a project porting Google's no longer supported **transitfeed** library to Python 3](https://github.com/pecalleja/transitfeed/tree/python3)

**gtfs-realtime.proto** comes from [GTFS Realtime Protobuf reference](https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto)
