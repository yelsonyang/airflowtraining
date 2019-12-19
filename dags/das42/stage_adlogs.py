import os
import logging
# import boto3

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta
# import package S3KeySensor
from airflow.operators import S3KeySensor

# custom utils
from das42.utils.job_config import JobConfig
from das42.utils.sql_utils import SqlUtils

JOB_ARGS = JobConfig.get_config()
DEFAULTS = JOB_ARGS["default_args"]
ENV = JOB_ARGS["env_name"]
TEAM_NAME = JOB_ARGS["team_name"]
SF_CONN_ID = JOB_ARGS["snowflake_conn_id"]
SF_ROLE = JOB_ARGS["snowflake"]["role"]
SF_WAREHOUSE = JOB_ARGS["snowflake"]["warehouse"]
SF_DATABASE = JOB_ARGS["snowflake"]["database"]

# reference variables here
BUCKET_NAME = JOB_ARGS["aws_rl_bucket_name"]
S3_CONN_ID = JOB_ARGS["aws_conn_id"]


# create DAG
DAG = DAG(
    "stage_simple",
    default_args=DEFAULTS,
    start_date=datetime(2018, 1, 1),
    schedule_interval=JOB_ARGS["schedule_interval"],
    catchup=False
)

stage_finish = DummyOperator(task_id="adlogs_snowflake_staging_finish")

# staging ad logs hourly
for table in JOB_ARGS["tables"]:

    stage_sql_path = os.path.join(
        JOB_ARGS["stage_sql_path"],
        table
        )

    query_log = SqlUtils.load_query(stage_sql_path).split("---")

    stage_adlogs_hourly_job = SnowflakeOperator(
        task_id="stage_logs_{}_hourly".format(table),
        snowflake_conn_id=SF_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=query_log,
        params={
            "env": ENV,
            "team_name": TEAM_NAME
        },
        autocommit=True,
        trigger_rule='all_done',
        dag=DAG
    )
    # add s3 sensor to check the presence of log files
    sensor = S3KeySensor(
        task_id="s3_key_sensor_task",
        bucket_name=BUCKET_NAME,
        bucket_key="das42-airflow-training-s3",
        s3_conn_id=S3_CONN_ID
    )

# set the order
sensor >> stage_adlogs_hourly_job >> stage_finish
