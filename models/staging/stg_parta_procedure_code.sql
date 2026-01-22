-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with parta_procedure_code as (
  SELECT
    *
  FROM
  {% if var('demo_data_only', false) %} {{ ref('parta_procedure_code') }} {% else %} {{ source('medicare_cclf','parta_procedure_code') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_TYPE_CD
    , CLM_VAL_SQNC_NUM
    , CLM_PRCDR_CD
    , CLM_PRCDR_PRFRM_DT
    , BENE_EQTBL_BIC_HICN_NUM
    , PRVDR_OSCAR_NUM
    , CLM_FROM_DT
    , CLM_THRU_DT
    , DGNS_PRCDR_ICD_IND
    , CLM_BLG_PRVDR_OSCAR_NUM
    , FILE_NAME
    , FILE_DATE
from parta_procedure_code