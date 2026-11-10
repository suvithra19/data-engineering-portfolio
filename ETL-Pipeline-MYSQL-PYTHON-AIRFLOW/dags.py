"""
Airflow DAG: runs our MySQL ETL every 5 minutes.

What is a DAG?  A "Directed Acyclic Graph" = a workflow made of tasks.
Here we have only ONE task: run the shell script that runs our Python ETL.

Flow:  Airflow Scheduler --> BashOperator --> run_etl.sh --> etl_script.py
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Settings shared by every task in this DAG
default_args = {
    "owner": "airflow",
    "retries": 2,                         # try once more if the task fails
    "retry_delay": timedelta(minutes=1),  # wait 1 minute before retrying
}

with DAG(
    dag_id="mysql_etl_dag",               # name shown in the Airflow UI
    description="A simple MySQL ETL DAG",
    default_args=default_args,
    start_date=datetime(2026, 10, 24),
    schedule=timedelta(minutes=5),        # run every 5 minutes
    catchup=False,                        # don't run for past missed dates
) as dag:

    run_etl = BashOperator(
        task_id="run_etl",
        bash_command="bash /home/suvithra/airflow/wrapper.sh ",
    )
