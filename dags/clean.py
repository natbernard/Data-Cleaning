import os
import sys


# airflow configurations
from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operator.bash import BashOperator
from airflow.utils.dates import days_ago

# Get the absolute path of the 'main' file
main_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_path)

main_file = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "main.py")
)


default_args = {
    "owner": "Natasha Bernard",
    "depends_on_past": False,
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0.5),
    "max_active_runs": 1,
}


# **********************AIRFLOW DAGS AND TASKS*********************
dag = DAG(
    "data_cleanup",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None
)


cleanup = BashOperator(
    task_id = "full_cleanup",
    bash_command =  "python3 '{0}'".format(main_file),
    dag = dag
)