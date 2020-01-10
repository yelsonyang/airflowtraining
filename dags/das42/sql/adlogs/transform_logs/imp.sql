create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.imp (
  record_type string,
  date string,
  time string,
  idevent_type string,
  placementid string,
  ipn string,
  idcreative string,
  config_id string,
  GUID string,
  geo_db string,
  section string,
  iab_flag string,
  ip_address string,
  rule_match string,
  keyword string,
  custom string,
  privacy string,
  placement_dt string,
  placement_path string,
  creative_dt string,
  creative_path string,
  domain string,
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

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.imp from (
  select distinct
    nullif(t.$1, '-') as record_type,
    nullif(t.$2, '-') as date,
    nullif(t.$3, '-') as time,
    nullif(t.$4, '-') as idevent_type,
    nullif(t.$5, '-') as placementid,
    nullif(t.$6, '-') as ipn,
    nullif(t.$7, '-') as idcreative,
    nullif(t.$8, '-') as config_id,
    nullif(t.$9, '-') as GUID,
    nullif(t.$10, '-') as geo_db,
    nullif(t.$11, '-') as section,
    nullif(t.$12, '-') as iab_flag,
    nullif(t.$13, '-') as ip_address,
    nullif(t.$14, '-') as rule_match,
    nullif(t.$15, '-') as keyword,
    nullif(t.$16, '-') as custom,
    nullif(t.$17, '-') as privacy,
    nullif(t.$18, '-') as placement_dt,
    nullif(t.$19, '-') as placement_path,
    nullif(t.$20, '-') as creative_dt,
    nullif(t.$21, '-') as creative_path,
    nullif(t.$22, '-') as domain,
    nullif(t.$23, '-') as parent_time,
    nullif(t.$24, '-') as device_id,
    nullif(t.$25, '-') as imp_id,
    nullif(t.$26, '-') as agent_env,
    nullif(t.$27, '-') as user_agent,
    nullif(t.$28, '-') as impression_guid,
    nullif(t.$29, '-') as tpplid,
    nullif(t.$30, '-') as idcampaign,
    nullif(t.$31, '-') as c2,
    nullif(t.$32, '-') as c3,
    metadata$filename as file_source,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as load_timestamp,
    2019070415 as run_datehour
  from @raw_stage/stage_imp_logs_{{ params.env }}/20190704/15/log/ t
)
file_format = raw_stage.log_csv_nh_format
on_error = continue
;
