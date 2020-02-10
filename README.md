# MBTA: My Best Transit App

The application collects General Transit Feed Specification (GTFS) Real-Time (RT) vehicle positions feeds (every 5 seconds) and GTFS schedule tables (once a day and only if there is an update) (both are published by the Massachusetts Bay Transportation Authority). The collected data are processed to provide hourly statistics on service delays (in seconds) through a user-friendly web-interface. The web-interface allows analysts accessing the data aggregated by routes, stops, or a combination thereof.

## Deployment Instructions

* Create a MySQL or Amazon Aurora (in MySQL mode) database either in Amazon Web Services RDS or on an EC2 instance. The database should be accessible from the EC2 machines to be created as described below.

* Create a Spark Cluster on Amazon Web Services using [the Pegasus tool](https://github.com/InsightDataScience/pegasus) as explained [here](https://blog.insightdatascience.com/how-to-get-hadoop-and-spark-up-and-running-on-aws-7a1b0ab55459)
  * If Python 3.6 is not installed on the cluster, install it by running `peg sshcmd-cluster spark_cluster sudo add-apt-repository ppa:fkrull/deadsnakes`, `peg sshcmd-cluster spark_cluster sudo apt update`, and `peg sshcmd-cluster spark_cluster sudo apt install python3.6-dev`
  * Install and upgrade **pip3** by running `peg sshcmd-cluster spark_cluster sudo apt install python3-pip -y` and `peg sshcmd-cluster spark_cluster sudo python3.6 -m pip install --upgrade pip`
  * Install **virtualenv** by running `peg sshcmd-cluster spark_cluster sudo python3.6 -m pip install virtualenv`
  * Create a virtual environment `peg sshcmd-cluster spark_cluster python3.6 -m virtualenv venv`
  * Upload the requirements.txt file for this project to the home directory of each Spark cluster machine and install all the required packages by running `peg sshcmd-cluster spark_cluster 'source /home/ubuntu/venv/bin/activate && pip install -r /home/ubuntu/requirements.txt'`
  * Set the Python for **pyspark** as follows `peg sshcmd-cluster spark_cluster 'sudo echo PYSPARK_PYTHON=/home/ubuntu/venv/bin/python >> /usr/local/spark/conf/spark-env.sh'` and `peg sshcmd-cluster spark_cluster 'sudo echo PYSPARK_DRIVER_PYTHON=/home/ubuntu/venv/bin/python >> /usr/local/spark/conf/spark-env.sh'`
  * Reboot the cluster `peg sshcmd-cluster spark_cluster sudo reboot` and start Spark `peg service spark_cluster spark start`

* Create and configure an Airflow server and 2 workers as described [here] (https://corecompete.com/scaling-out-airflow-with-celery-and-rabbitmq-to-orchestrate-etl-jobs-on-the-cloud)
  * Create an additional queue named _sparks_ for Spark tasks
  * The second worker needs to be additionally configured by editing the file $AIRFLOW_HOME/airflow.cfg and setting the value `default_queue = sparks`
  * Place the file _air_dagbag.py_ in the directory $AIRFLOW_HOME/dags on all the Airflow machines
  * Run `airflow list_dags` and check that the three DAGs for the project are discovered by Airflow on each of the Airflow machines
  * Make sure that the following environment variables are set on each Airflow machine: MYSQL_HOST, MYSQL_USER, MYSQL_PWD, MYSQL_DBNAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
  * Start the Airflow server and workers as described at the link above
  
* Configure a Flask webserver on an additional Amazon Web Services EC2 instance
  * Make sure that the following environment variables are set on the webserver machine: MYSQL_HOST, MYSQL_USER, MYSQL_PWD, MYSQL_DBNAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

## Packages and Files

The tool is implemented as a set of scripts for Python 3. Frontend, in addition, contains some HTML, CSS, and Javascript files. The folder structure is as follows

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
**air_dagbag.py** | Allows Airflow to discover task definition graphs in the **airdags/** folder, should be deployed to $AIRFLOW_HOME/dags and modified as described in the file to point to the **airdags/** folder
**frontend_app.py** | Flask application start-up script
**myspark.sh** | Helper Bash script to launch Spark jobs

## Author

* **Alex Ganin** - *Initial work* - [alxga](https://github.com/alxga)

## License

This project is licensed under the MIT License
