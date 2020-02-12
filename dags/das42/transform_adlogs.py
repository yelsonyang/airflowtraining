import os
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta

# custom utils
from das42.utils.job_config import JobConfig
from das42.utils.sql_utils import SqlUtils

# import package S3KeySensor
from airflow.operators import S3KeySensor
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.sensors import ExternalTaskSensor



JOB_ARGS = JobConfig.get_config()
DEFAULTS = JOB_ARGS["default_args"]
ENV = JOB_ARGS["env_name"]
TEAM_NAME = JOB_ARGS["team_name"]
SF_CONN_ID = JOB_ARGS["snowflake_conn_id"]
SF_ROLE = JOB_ARGS["snowflake"]["role"]
SF_WAREHOUSE = JOB_ARGS["snowflake"]["warehouse"]
SF_DATABASE = JOB_ARGS["snowflake"]["database"]
BUCKET_NAME = JOB_ARGS["aws_rl_bucket_name"]
AWS_CONN_ID = JOB_ARGS["aws_conn_id"]

# create a new transform DAG
DAG = DAG(
    "transform_adlog",
    default_args=DEFAULTS,
    start_date=datetime(2018, 1, 1),
    schedule_interval=JOB_ARGS["schedule_interval"],
    catchup=False
)

transform_finish = DummyOperator(task_id="trasnform_staging_finish")

for table in JOB_ARGS["tables"]:

    dag_dependency = ExternalTaskSensor(
    task_id="previous_dag_complete_{}".format(table),
    external_dag_id = 'stage_simple_adlog',
    external_task_id = "transform_logs_{}_hourly".format(table)
    )

    completed_process_log = []

    for process in JOB_ARGS["tables"][table]:

        process_path = os.path.join(
            JOB_ARGS["transform_log_path"],
            process
            )

        process_log = SqlUtils.load_query(process_path) # string
        completed_process_log.append(process_log) # nested list

        subList = [elem.split("---") for elem in completed_process_log]
        newList = [l for elem in subList for l in elem] 

    transform_adlogs_job = SnowflakeOperator(
        task_id="transform_log_{}_process".format(table),
        snowflake_conn_id=SF_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=newList,
        params={
            "env": ENV,
            "team_name": TEAM_NAME,
            "table": table
        },
        autocommit=True,
        trigger_rule='all_done',
        dag=DAG
    )

# set the order
    dag_dependency >> transform_adlogs_job >> transform_finish
