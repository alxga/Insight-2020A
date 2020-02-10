# MBTA: My Best Transit App

The application collects General Transit Feed Specification (GTFS) Real-Time (RT) vehicle positions feeds (every 5 seconds) and GTFS schedule tables (once a day and only if there is an update) (both are published by the Massachusetts Bay Transportation Authority). The collected data are processed to provide hourly statistics on service delays (in seconds) through a user-friendly web-interface. The web-interface allows analysts accessing the data aggregated by routes, stops, or a combination thereof.

## Implementation and Source Files and Folders

The tool is implemented as a set of scripts for Python 3. Frontend, in addition, contains some HTML, CSS, and Javascript files. The folder structure is described as follows

File or Package | Role
---- | ----
**.vscode/** | Visual Studio Code project configuration files
**airdags/** | Airflow task definition graphs
**airtasks/** | Airflow tasks scripts
**common/** | Helper classes and functions shared among worker tasks and the frontend
**frontend/** | Frontend Flask web app
**third_party/** | Modified 3rd-party packages and files
**.flaskenv** | Flask dot-environment file, defines FLASK_APP
**.gitignore** | Defines path patterns ignored by Git
**air_dagbag.py** | The file should be deployed to $AIRFLOW_HOME/dags and modified to point to the **airdags/** folder
**frontend_app.py** | Flask application start-up script
**myspark.sh** | Helper Bash script to launch Spark jobs

## Author

* **Alex Ganin** - *Initial work* - [alxga](https://github.com/alxga)

## License

This project is licensed under the MIT License
