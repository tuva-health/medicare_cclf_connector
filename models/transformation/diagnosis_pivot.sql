with diagnois_pivot as (

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind
        , {{ dbt_utils.pivot(
              column='clm_val_sqnc_num'
            , values=dbt_utils.get_column_values(ref('base_parta_diagnosis_code'), 'clm_val_sqnc_num')
            , agg='max'
            , then_value='clm_dgns_cd'
            , else_value='null'
            , prefix='diagnosis_code_'
            , quote_identifiers=false
          ) }}
    from {{ ref('base_parta_diagnosis_code') }}
    group by
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind

),

poa_pivot as (

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind
        , {{ dbt_utils.pivot(
              column='clm_val_sqnc_num'
            , values=dbt_utils.get_column_values(ref('base_parta_diagnosis_code'), 'clm_val_sqnc_num')
            , agg='max'
            , then_value='clm_poa_ind'
            , else_value='null'
            , prefix='diagnosis_poa_'
            , quote_identifiers=false
          ) }}
    from {{ ref('base_parta_diagnosis_code') }}
    group by
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind

)

select
      dx.cur_clm_uniq_id
    , dx.bene_mbi_id
    , dx.dgns_prcdr_icd_ind
    , dx.diagnosis_code_1
    , dx.diagnosis_code_2
    , dx.diagnosis_code_3
    , dx.diagnosis_code_4
    , dx.diagnosis_code_5
    , dx.diagnosis_code_6
    , dx.diagnosis_code_7
    , dx.diagnosis_code_8
    , dx.diagnosis_code_9
    , dx.diagnosis_code_10
    , dx.diagnosis_code_11
    , dx.diagnosis_code_12
    , dx.diagnosis_code_13
    , dx.diagnosis_code_14
    , dx.diagnosis_code_15
    , dx.diagnosis_code_16
    , dx.diagnosis_code_17
    , dx.diagnosis_code_18
    , dx.diagnosis_code_19
    , dx.diagnosis_code_20
    , dx.diagnosis_code_21
    , dx.diagnosis_code_22
    , dx.diagnosis_code_23
    , dx.diagnosis_code_24
    , dx.diagnosis_code_25
    , poa.diagnosis_poa_1
    , poa.diagnosis_poa_2
    , poa.diagnosis_poa_3
    , poa.diagnosis_poa_4
    , poa.diagnosis_poa_5
    , poa.diagnosis_poa_6
    , poa.diagnosis_poa_7
    , poa.diagnosis_poa_8
    , poa.diagnosis_poa_9
    , poa.diagnosis_poa_10
    , poa.diagnosis_poa_11
    , poa.diagnosis_poa_12
    , poa.diagnosis_poa_13
    , poa.diagnosis_poa_14
    , poa.diagnosis_poa_15
    , poa.diagnosis_poa_16
    , poa.diagnosis_poa_17
    , poa.diagnosis_poa_18
    , poa.diagnosis_poa_19
    , poa.diagnosis_poa_20
    , poa.diagnosis_poa_21
    , poa.diagnosis_poa_22
    , poa.diagnosis_poa_23
    , poa.diagnosis_poa_24
    , poa.diagnosis_poa_25
from diagnois_pivot as dx
inner join poa_pivot as poa
	on dx.cur_clm_uniq_id = poa.cur_clm_uniq_id
/* filtering out null values from seed file */
where dx.cur_clm_uniq_id is not null