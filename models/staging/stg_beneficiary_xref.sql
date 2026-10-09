with beneficiary_xref as (
  SELECT 
    *
  FROM
  {{ source('medicare_cclf','beneficiary_xref') }}
)

select
      HICN_MBI_XREF_IND
    , CRNT_NUM
    , PRVS_NUM
    , {{ try_to_cast_date('PRVS_ID_EFCTV_DT') }} as PRVS_ID_EFCTV_DT
    , {{ try_to_cast_date('PRVS_ID_OBSLT_DT') }} as PRVS_ID_OBSLT_DT
    , BENE_RRB_NUM
    , FILE_NAME
    , {{ try_to_cast_date('FILE_DATE') }} as FILE_DATE
from beneficiary_xref
