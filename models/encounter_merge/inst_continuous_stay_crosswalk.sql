
{{ config(materialized='table') }}

/** Identify claims of the same type for the same patient that have the same from date  
		*via research, these claims represent a continuous stay that is charged over time  **/
with population_stage as(
  select 
    h.bene_mbi_id
    ,h.clm_type_cd
    ,dense_rank() over (partition by h.bene_mbi_id,h.clm_type_cd order by h.clm_from_dt) as claim_grouper
    ,h.clm_from_dt
  from {{ ref('inst_claims_prep')}} h
  where 1=1
  and h.clm_type_cd <> '40' -- No outpatient
)
/**  Group together claim types for the same patient on the same day that have been ranked together.  
		Then filter on those with multiple counts (meaning multiple claims)
**/
, population as(
  select 
    claim_grouper
    ,bene_mbi_id
    ,clm_from_dt
    ,clm_type_cd
    ,count(*)
  from population_stage 
  group by 
    claim_grouper
    ,bene_mbi_id
    ,clm_from_dt
    ,clm_type_cd
  having count(*) > 1
)
/**  For the continuous stay population only, regrouping claims  **/
,continuous_stay as(
  select 
    h.bene_mbi_id || h.clm_type_cd || isnull(h.prncpl_dgns_cd,'')  || isnull(h.fac_prvdr_npi_num,'')  || isnull(h.atndg_prvdr_npi_num,'') || isnull(r.clm_line_prod_rev_ctr_cd,'')  || replace(cast(h.clm_thru_dt as date),'-','') || replace(cast(ISNULL(h.clm_from_dt,'1900-01-01') as date),'-','')
    as encounter_id
    ,h.bene_mbi_id
    ,h.clm_type_cd
    ,dense_rank() over (partition by h.bene_mbi_id,h.clm_type_cd order by h.clm_from_dt) as claim_grouper
    ,h.cur_clm_uniq_id
    ,h.clm_from_dt
    ,h.clm_thru_dt
  from {{ ref('inst_claims_prep')}} h
  inner join {{ source('medicare_cclf','parta_claims_revenue_center_detail')}} r
  on h.cur_clm_uniq_id = r.cur_clm_uniq_id
  and r.clm_line_num = 1
  inner join population p
  on h.bene_mbi_id = p.bene_mbi_id
  and h.clm_from_dt = p.clm_from_dt
  and h.clm_type_cd = p.clm_type_cd
  where 1=1

)

,encounter_group as(
  select 
    MAX(encounter_id) as encounter_id
    ,claim_grouper
    ,bene_mbi_id
    ,clm_type_cd
    ,clm_from_dt
  from continuous_stay
  group by
    claim_grouper
    ,bene_mbi_id
    ,clm_type_cd
    ,clm_from_dt
)

select 
  e.encounter_id
  ,c.bene_mbi_id
  ,c.cur_clm_uniq_id 
from encounter_group e
inner join continuous_stay c
	on e.claim_grouper = c.claim_grouper
  and e.bene_mbi_id = c.bene_mbi_id
  and e.clm_type_cd = c.clm_type_cd
  and e.clm_from_dt = c.clm_from_dt