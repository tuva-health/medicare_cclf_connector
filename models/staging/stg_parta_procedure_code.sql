select
      cur_clm_uniq_id
    , bene_mbi_id
    , bene_hic_num
    , clm_type_cd
    , clm_val_sqnc_num
    , clm_prcdr_cd
    , clm_prcdr_prfrm_dt
    , bene_eqtbl_bic_hicn_num
    , prvdr_oscar_num
    , clm_from_dt
    , clm_thru_dt
    , dgns_prcdr_icd_ind
    , file_name
    , file_date
from {{ source('medicare_cclf','parta_procedure_code') }}