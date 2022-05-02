
select 
    max(h1.encounter_id) as encounter_id
    ,h1.cur_clm_uniq_id
    ,h1.bene_mbi_id
from {{ ref('inst_claims_unique')}} h1
inner join {{ ref('inst_claims_unique')}} h2
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