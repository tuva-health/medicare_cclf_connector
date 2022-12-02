with procedures as (

    select
          {{ cast_string_or_varchar('cur_clm_uniq_id') }} as cur_clm_uniq_id
        , {{ cast_string_or_varchar('bene_mbi_id') }} as bene_mbi_id
        , {{ cast_string_or_varchar('bene_hic_num') }} as bene_hic_num
        , {{ cast_string_or_varchar('clm_type_cd') }} as clm_type_cd
        , {{ cast_string_or_varchar('clm_val_sqnc_num') }} as clm_val_sqnc_num
        , {{ cast_string_or_varchar('clm_prcdr_cd') }} as clm_prcdr_cd
        , {{ cast_string_or_varchar('clm_prcdr_prfrm_dt') }} as clm_prcdr_prfrm_dt
        , {{ cast_string_or_varchar('bene_eqtbl_bic_hicn_num') }} as bene_eqtbl_bic_hicn_num
        , {{ cast_string_or_varchar('prvdr_oscar_num') }} as prvdr_oscar_num
        , {{ cast_string_or_varchar('clm_from_dt') }} as clm_from_dt
        , {{ cast_string_or_varchar('clm_thru_dt') }} as clm_thru_dt
        , {{ cast_string_or_varchar('dgns_prcdr_icd_ind') }} as dgns_prcdr_icd_ind
    from {{ var('parta_procedure_code') }}

),

procedure_seed as (

    select
          {{ cast_string_or_varchar('cur_clm_uniq_id') }} as cur_clm_uniq_id
        , {{ cast_string_or_varchar('bene_mbi_id') }} as bene_mbi_id
        , {{ cast_string_or_varchar('bene_hic_num') }} as bene_hic_num
        , {{ cast_string_or_varchar('clm_type_cd') }} as clm_type_cd
        , {{ cast_string_or_varchar('clm_val_sqnc_num') }} as clm_val_sqnc_num
        , {{ cast_string_or_varchar('clm_prcdr_cd') }} as clm_prcdr_cd
        , {{ cast_string_or_varchar('clm_prcdr_prfrm_dt') }} as clm_prcdr_prfrm_dt
        , {{ cast_string_or_varchar('bene_eqtbl_bic_hicn_num') }} as bene_eqtbl_bic_hicn_num
        , {{ cast_string_or_varchar('prvdr_oscar_num') }} as prvdr_oscar_num
        , {{ cast_string_or_varchar('clm_from_dt') }} as clm_from_dt
        , {{ cast_string_or_varchar('clm_thru_dt') }} as clm_thru_dt
        , {{ cast_string_or_varchar('dgns_prcdr_icd_ind') }} as dgns_prcdr_icd_ind
    from {{ ref('parta_procedure_code_seed') }}

),

/*
    union with seed file to ensure that all procedure code/date columns are
    included during the pivot operation in the transformation step
*/
unioned as (

    select * from procedures
    union all
    select * from procedure_seed

)

select * from unioned