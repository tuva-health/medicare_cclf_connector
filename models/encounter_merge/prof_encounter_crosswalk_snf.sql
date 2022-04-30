select distinct
      p.cur_clm_uniq_id
      ,p.bene_mbi_id
      ,i.encounter_id
from {{ source('medicare_cclf','partb_physicians')}} p
inner join {{ ref('inst_claims_final')}} i
  on p.bene_mbi_id = i.bene_mbi_id
  and p.clm_thru_dt >= i.clm_from_dt
  and p.clm_thru_dt <= i.clm_thru_dt
where 1=1
  and i.clm_type_cd in ('20'   -- non-swing SNF
                       ,'30'   -- swing SNF
                       )
  and p.clm_pos_cd = '31'      -- snf

