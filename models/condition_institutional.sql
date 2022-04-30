with admit_diag as(
  select 
     cast(encounter_id as varchar) as encounter_id
    , cast(bene_mbi_id as varchar) as patient_id
    , cast(clm_from_dt as datetime) as condition_date
    , cast('admit diagnosis' as varchar) as condition_type
      , cast(case dgns_prcdr_icd_ind
            when 0 then 'icd-10-cm'
            when 9 then 'icd-9-cm'
      end as varchar) as code_type
    , cast(admtg_dgns_cd as varchar) as code
    , cast(NULL as varchar) as description
    , cast(2 as int) as diagnosis_rank
    , cast(NULL as varchar) as present_on_admit
    , cast('cclf' as varchar) as data_source
  from {{ ref('inst_claims_final')}}
)

select
   cast(f.encounter_id as varchar) as encounter_id
  , cast(dx.bene_mbi_id as varchar) as patient_id
  , cast(dx.clm_from_dt as datetime) as condition_date
  , cast('problem' as varchar) as condition_type
  , cast(case dx.dgns_prcdr_icd_ind
          when 0 then 'icd-10-cm'
          when 9 then 'icd-9-cm'
    end as varchar) as code_type
  , cast(dx.clm_dgns_cd as varchar) as code
  , cast(NULL as varchar) as description
  , cast(dx.clm_val_sqnc_num as int) as diagnosis_rank
  , cast(dx.clm_poa_ind as varchar) as present_on_admit
  , cast('cclf' as varchar) as data_source
from {{ source('medicare_cclf','parta_diagnosis_code')}} dx
inner join {{ ref('inst_claims_final')}} f
	on dx.cur_clm_uniq_id = f.cur_clm_uniq_id

union all

  select 
     encounter_id
    , patient_id
    , condition_date
    , condition_type
    , code_type
    , code
    , description
    , diagnosis_rank
    , present_on_admit
    , data_source
  from admit_diag