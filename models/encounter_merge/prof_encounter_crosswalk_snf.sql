with crosswalk as(
  select distinct
        p.cur_clm_uniq_id
        ,p.bene_mbi_id
        ,i.encounter_id
  from {{ source('medicare_cclf','partb_physicians')}} p
  inner join {{ ref('inst_claims_final')}} i
    on p.bene_mbi_id = i.bene_mbi_id
    and p.clm_thru_dt >= i.clm_from_dt
    and p.clm_thru_dt <= i.clm_thru_dt
    and p.clm_line_num = '1'
  where 1=1
    and i.clm_type_cd in ('20'   -- non-swing SNF
                         ,'30'   -- swing SNF
                         )
    and p.clm_pos_cd = '31'      -- snf
)
, duplicates as(
  select
    cur_clm_uniq_id
    ,bene_mbi_id
    ,encounter_id
    ,row_number() over (partition by cur_clm_uniq_id, bene_mbi_id order by encounter_id) as row_number
  from crosswalk
)

select 
  cur_clm_uniq_id
  ,bene_mbi_id
  ,encounter_id
from duplicates
where row_number = 1


 