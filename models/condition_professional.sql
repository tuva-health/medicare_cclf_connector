select
     cast(encounter_id as varchar) as encounter_id
    , cast(patient_id as varchar) as patient_id
    , cast(condition_date as varchar) as condition_date
    , cast(condition_type as varchar) as condition_type
    , cast(code_type as varchar) as code_type
    , cast(code as varchar) as code
    , cast(description as varchar) as description
    , cast(diagnosis_rank as varchar) as diagnosis_rank
    , cast(present_on_admit as varchar) as present_on_admit
    , cast(data_source as varchar) as data_source
from(
  select
      encounter_id
      , bene_mbi_id as patient_id
      , clm_from_dt as condition_date
      , 'problem' as condition_type
      , case dgns_prcdr_icd_ind
         when 0 then 'icd-10-cm'
         when 9 then 'icd-9-cm'
      end as code_type
      , code as code
      , NULL as description
      , replace(replace(diagnosis_sequence,'clm_dgns_',''),'_cd','') as diagnosis_rank
      , NULL as present_on_admit
      , 'cclf' as data_source
        from {{ ref('prof_claims_final')}}
        unpivot(
          code for diagnosis_sequence in (CLM_DGNS_1_CD
                                          ,CLM_DGNS_2_CD
                                          ,CLM_DGNS_3_CD
                                          ,CLM_DGNS_4_CD
                                          ,CLM_DGNS_5_CD
                                          ,CLM_DGNS_6_CD
                                          ,CLM_DGNS_7_CD
                                          ,CLM_DGNS_8_CD
                                          ,CLM_DGNS_9_CD
                                          ,CLM_DGNS_10_CD
                                          ,CLM_DGNS_11_CD
                                          ,CLM_DGNS_12_CD
            )
          )x
		)