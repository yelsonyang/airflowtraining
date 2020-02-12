
update airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{ params.table }}
set iab_flag = 's'
where (
    (
    placementid = 0
    or section is null )
    and iab_flag in ('w', 'x'));

---

create or replace table airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{ params.table }}_purged_smartclip_rows as
(
select
    placementid,
    idcampaign,
    idevent_type,
    idcreative,
    coalesce(section,'') as section
from
    airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{ params.table }}
where
    iab_flag = 's'
);

---

update airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.{{ params.table }} as c
set placementid = sc.placementid
from dimensions.placement_smartclip sc
where
    sc.unhex_md5_smartclip = c.unhex_md5_smartclip;
