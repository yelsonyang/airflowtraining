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

stage_finish = DummyOperator(task_id="trasnform_staging_finish")

for table in JOB_ARGS["tables"]:
    for process in JOB_ARGS["tables"][table]:

        transform_sql_path = os.path.join(
            JOB_ARGS["transform_sql_path"],
            table
            )

        # set the sql path for all 3 transformation processes
        process_path = os.path.join(
            JOB_ARGS["transform_log_path"],
            process,
            )

        process_log = SqlUtils.load_query(process_path).split("---")

        transform_adlogs_job = SnowflakeOperator(
            task_id="transform_log_{}_{}".format(table).format(process),
            snowflake_conn_id=SF_CONN_ID,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            sql=process_log,
            params={
                "env": ENV,
                "team_name": TEAM_NAME
            },
            autocommit=True,
            trigger_rule='all_done',
            dag=DAG
        )

    # set the order
    transform_adlogs_job >> stage_finish
