select
      hicn_mbi_xref_ind
    , crnt_num
    , prvs_num
    , prvs_id_efctv_dt
    , prvs_id_obslt_dt
    , bene_rrb_num
    , file_name
    , file_date
from {{ source('medicare_cclf','beneficiary_xref') }}