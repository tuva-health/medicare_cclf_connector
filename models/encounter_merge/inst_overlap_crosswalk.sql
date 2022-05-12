with population as(
select 
    p.encounter_id
    ,p.bene_mbi_id
    ,p.cur_clm_uniq_id
  	,p.clm_type_cd
  	,p.fac_prvdr_npi_num
  	,p.clm_from_dt
  	,p.clm_thru_dt
from {{ ref('inst_claims_prep')}} p
left join {{ ref('inst_continuous_stay_crosswalk')}} c
  on p.cur_clm_uniq_id = c.cur_clm_uniq_id
where c.cur_clm_uniq_id is null
 )

select 
    max(h1.encounter_id) as encounter_id
    ,h1.bene_mbi_id
    ,h1.cur_clm_uniq_id
from population h1
inner join population h2
	on h1.bene_mbi_id = h2.bene_mbi_id
    and h1.clm_type_cd = h2.clm_type_cd
    and h1.fac_prvdr_npi_num = h2.fac_prvdr_npi_num
    and h1.cur_clm_uniq_id <> h2.cur_clm_uniq_id
where 1=1
and h1.clm_type_cd <> '40' 
and ((h2.clm_from_dt > h1.clm_from_dt and h2.clm_from_dt < h1.clm_thru_dt)
		or (h2.clm_thru_dt > h1.clm_from_dt and h2.clm_thru_dt < h1.clm_thru_dt))
group by
    h1.cur_clm_uniq_id
    ,h1.bene_mbi_id