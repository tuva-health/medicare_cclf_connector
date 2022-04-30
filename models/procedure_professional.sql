with hcpcs_pivot as(
  select
     cast(encounter_id as varchar) as encounter_id
  	, cur_clm_uniq_id
  	, clm_line_num
    , cast(bene_mbi_id as varchar) as patient_id
    , cast(clm_from_dt as datetime) as procedure_date
    , cast('hcpcps' as varchar) as code_type
    , cast(code as varchar) as code
    , cast(NULL as varchar) as description
    , cast(NULL as varchar) as physician_npi
    , cast('cclf' as varchar) as data_source
    from {{ ref('prof_claims_final')}}
    	unpivot(
          code for diagnosis_sequence in (clm_line_hcpcs_cd)
          )hcpcs
  )
, provider_pivot as (
  select 
  cur_clm_uniq_id
  , clm_line_num
  , physician_npi
  from {{ ref('prof_claims_final')}}
  	unpivot(
    	physician_npi for provider in (rndrg_prvdr_npi_num)
		)npi
)


select
   cast(h.encounter_id as varchar) as encounter_id
  , cast(h.patient_id as varchar) as patient_id
  , cast(h.procedure_date as datetime) as procedure_date
  , cast(h.code_type as varchar) as code_type
  , cast(h.code as varchar) as code
  , cast(h.description as varchar) as description
  , cast(p.physician_npi as varchar) as physician_npi
  , cast(h.data_source as varchar) as data_source
from hcpcs_pivot h
left join provider_pivot p
	on h.cur_clm_uniq_id = p.cur_clm_uniq_id
    and h.clm_line_num = p.clm_line_num