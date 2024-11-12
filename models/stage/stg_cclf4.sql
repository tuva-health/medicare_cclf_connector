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
    , null as file_row_number
    , ingest_datetime
from {{ source('medicare_cclf','parta_diagnosis_code') }}