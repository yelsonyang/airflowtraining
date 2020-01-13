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

# table_group_1 = table[imp, click, state]
# table_group_2 = [imp, click]
# table_group_3 = [int]

# recursive table jobs for the stage_simple_adlog
for table in JOB_ARGS["tables"]:

    transform_sql_path = os.path.join(
        JOB_ARGS["transform_sql_path"],
        table
        )

    # ipn_blacklist process
    ipn_blacklist_path = os.path.join(
        JOB_ARGS["transform_log_path"],
        "ipn_blacklist",
        )

    transform_ipn_blacklist_log = SqlUtils.load_query(ipn_blacklist_path).split("---")

    # table_group_1 = table[1, 0, 5]

    transform_ipn_blacklist_job = SnowflakeOperator(
        task_id="transform_ipn_blacklist_{}".format(table),
        snowflake_conn_id=SF_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=transform_ipn_blacklist_log,
        params={
            "env": ENV,
            "team_name": TEAM_NAME
        },
        autocommit=True,
        trigger_rule='all_done',
        dag=DAG
    )

    # set the order
    transform_ipn_blacklist_job >> stage_finish
