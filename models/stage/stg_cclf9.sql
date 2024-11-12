select
      hicn_mbi_xref_ind
    , crnt_num
    , prvs_num
    , prvs_id_efctv_dt
    , prvs_id_obslf_dt as  prvs_id_obslt_dt
    , bene_rrb_num
    , file_name
    , null as file_row_number
    , ingest_datetime
from {{ source('medicare_cclf','beneficiary_xref') }}