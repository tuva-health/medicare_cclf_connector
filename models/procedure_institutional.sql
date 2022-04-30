select
   cast(encounter_id as varchar) as encounter_id
  , cast(c.bene_mbi_id as varchar) as patient_id
  , cast(c.clm_prcdr_prfrm_dt as datetime) as procedure_date
  , cast(case c.dgns_prcdr_icd_ind
    		when 0 then 'icd-10-cm'
    		when 9 then 'icd-9-cm'
    end as varchar) as code_type
  , cast(c.clm_prcdr_cd as varchar) as code
  , cast(NULL as varchar) as description
  , cast(h.oprtg_prvdr_npi_num as varchar) as physician_npi
  , cast('cclf' as varchar) as data_source
from {{ source('medicare_cclf','parta_procedure_code')}} c
left join {{ ref('inst_claims_final')}}h
	on c.cur_clm_uniq_id = h.cur_clm_uniq_id


