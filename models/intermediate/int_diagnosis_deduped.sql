with staged_data as (

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , bene_hic_num
        , clm_type_cd
        , clm_prod_type_cd
        , clm_val_sqnc_num
        , clm_dgns_cd
        , bene_eqtbl_bic_hicn_num
        , prvdr_oscar_num
        , clm_from_dt
        , clm_thru_dt
        , clm_poa_ind
        , dgns_prcdr_icd_ind
        , file_name
        , file_date
    from {{ ref('stg_parta_diagnosis_code') }}

)

/* dedupe full rows that may appear in multiple files */
, add_row_num as (

    select *, row_number() over (
        partition by
              cur_clm_uniq_id
            , bene_mbi_id
            , bene_hic_num
            , clm_type_cd
            , clm_prod_type_cd
            , clm_val_sqnc_num
            , clm_dgns_cd
            , bene_eqtbl_bic_hicn_num
            , prvdr_oscar_num
            , clm_from_dt
            , clm_thru_dt
            , clm_poa_ind
            , dgns_prcdr_icd_ind
        order by file_date desc
        ) as row_num
    from staged_data
    where bene_mbi_id is not null /* added to prevent dupes during pivot */

)

/* cast data types before pivot operation */
select
      cast(cur_clm_uniq_id as {{ dbt.type_string() }} ) as cur_clm_uniq_id
    , cast(bene_mbi_id as {{ dbt.type_string() }} ) as bene_mbi_id
    , cast(bene_hic_num as {{ dbt.type_string() }} ) as bene_hic_num
    , cast(clm_type_cd as {{ dbt.type_string() }} ) as clm_type_cd
    , cast(clm_prod_type_cd as {{ dbt.type_string() }} ) as clm_prod_type_cd
    , cast(clm_val_sqnc_num as {{ dbt.type_string() }} ) as clm_val_sqnc_num
    , cast(clm_dgns_cd as {{ dbt.type_string() }} ) as clm_dgns_cd
    , cast(bene_eqtbl_bic_hicn_num as {{ dbt.type_string() }} ) as bene_eqtbl_bic_hicn_num
    , cast(prvdr_oscar_num as {{ dbt.type_string() }} ) as prvdr_oscar_num
    , cast(clm_from_dt as {{ dbt.type_string() }} ) as clm_from_dt
    , cast(clm_thru_dt as {{ dbt.type_string() }} ) as clm_thru_dt
    , cast(clm_poa_ind as {{ dbt.type_string() }} ) as clm_poa_ind
    , cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) as dgns_prcdr_icd_ind
    , file_name
    , file_date
from add_row_num
where row_num = 1