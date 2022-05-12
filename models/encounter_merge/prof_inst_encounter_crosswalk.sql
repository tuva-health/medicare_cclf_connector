with crosswalk_stage as(
  select
    p.cur_clm_uniq_id
    ,p.clm_line_num
    ,p.bene_mbi_id
    ,p.clm_pos_cd
    ,tp.description
    ,i.encounter_id
    ,i.encounter_type
    ,p.clm_thru_dt
    ,p.clm_from_dt
    ,i.encounter_start_date
    ,i.encounter_end_date
  from {{ source('medicare_cclf','partb_physicians')}} p
  inner join {{ ref('inst_encounter_core')}} i
        on p.bene_mbi_id = i.patient_id
        and p.clm_from_dt >= i.encounter_start_date
        and p.clm_thru_dt <= i.encounter_end_date
  left join {{ ref('place_of_service')}} tp
      on p.clm_pos_cd = tp.place_of_service_code
  --where clm_pos_cd <> '11'
)
, dupes as(
  select   
    cur_clm_uniq_id
    ,clm_line_num 
  from crosswalk_stage
  group by
    cur_clm_uniq_id
    ,clm_line_num
  having count(*) > 1
)

select
  s.encounter_id
  ,s.cur_clm_uniq_id
  ,s.clm_line_num
  ,s.bene_mbi_id
from crosswalk_stage s
left join dupes d
on s.cur_clm_uniq_id = d.cur_clm_uniq_id
and s.clm_line_num = d.clm_line_num
where d.cur_clm_uniq_id is null