create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.click (
  record_type string,
  date string,
  time string,
  idevent_type string,
  placementid string,
  ipn string,
  idcreative string,
  configuration_id string,
  GUID string,
  iab_flag string,
  ip_address string,
  rule_match string,
  custom string,
  section string,
  keyword string,
  privacy string,
  parent_time string,
  device_id string,
  imp_id string,
  agent_env string,
  user_agent string,
  impression_guid string,
  unhex_md5_smartclip string, --this will be unhex_md5_smartclip
  idcampaign string,
  c2 string,
  c3 string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
  )
  ;

  ---

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.click
select *
from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.click
where run_datehour = {{execution_date.strftime("%Y%m%d%H")}}
;
