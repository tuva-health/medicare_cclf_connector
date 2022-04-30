
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
  and i.clm_type_cd in ('50')
  and p.clm_pos_cd in ('34')

