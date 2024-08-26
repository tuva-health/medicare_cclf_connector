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
from {{ source('medicare_cclf','parta_diagnosis_code') }}