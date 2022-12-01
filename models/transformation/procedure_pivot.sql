with procedure_pivot as (

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind
        , {{ dbt_utils.pivot(
              column='clm_val_sqnc_num'
            , values=dbt_utils.get_column_values(ref('base_parta_procedure_code'), 'clm_val_sqnc_num')
            , agg='max'
            , then_value='clm_prcdr_cd'
            , else_value='null'
            , prefix='procedure_code_'
            , quote_identifiers=false
          ) }}
    from {{ ref('base_parta_procedure_code') }}
    group by
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind

),

date_pivot as(

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind
        , {{ dbt_utils.pivot(
              column='clm_val_sqnc_num'
            , values=dbt_utils.get_column_values(ref('base_parta_procedure_code'), 'clm_val_sqnc_num')
            , agg='max'
            , then_value='clm_prcdr_prfrm_dt'
            , else_value='null'
            , prefix='procedure_date_'
            , quote_identifiers=false
          ) }}
    from {{ ref('base_parta_procedure_code') }}
    group by
          cur_clm_uniq_id
        , bene_mbi_id
        , dgns_prcdr_icd_ind

)

select
      px.cur_clm_uniq_id
    , px.bene_mbi_id
    , px.dgns_prcdr_icd_ind
    , px.procedure_code_1
    , px.procedure_code_2
    , px.procedure_code_3
    , px.procedure_code_4
    , px.procedure_code_5
    , px.procedure_code_6
    , px.procedure_code_7
    , px.procedure_code_8
    , px.procedure_code_9
    , px.procedure_code_10
    , px.procedure_code_11
    , px.procedure_code_12
    , px.procedure_code_13
    , px.procedure_code_14
    , px.procedure_code_15
    , px.procedure_code_16
    , px.procedure_code_17
    , px.procedure_code_18
    , px.procedure_code_19
    , px.procedure_code_20
    , px.procedure_code_21
    , px.procedure_code_22
    , px.procedure_code_23
    , px.procedure_code_24
    , px.procedure_code_25
    , d.procedure_date_1
    , d.procedure_date_2
    , d.procedure_date_3
    , d.procedure_date_4
    , d.procedure_date_5
    , d.procedure_date_6
    , d.procedure_date_7
    , d.procedure_date_8
    , d.procedure_date_9
    , d.procedure_date_10
    , d.procedure_date_11
    , d.procedure_date_12
    , d.procedure_date_13
    , d.procedure_date_14
    , d.procedure_date_15
    , d.procedure_date_16
    , d.procedure_date_17
    , d.procedure_date_18
    , d.procedure_date_19
    , d.procedure_date_20
    , d.procedure_date_21
    , d.procedure_date_22
    , d.procedure_date_23
    , d.procedure_date_24
    , d.procedure_date_25
from procedure_pivot px
inner join date_pivot d
	on px.cur_clm_uniq_id = d.cur_clm_uniq_id
/* filtering out null values from seed file */
where px.cur_clm_uniq_id is not null