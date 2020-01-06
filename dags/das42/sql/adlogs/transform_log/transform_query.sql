drop schema if exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}
create schema airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }} clone airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}
