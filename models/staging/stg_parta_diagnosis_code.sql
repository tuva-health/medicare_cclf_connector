-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with parta_diagnosis_code as (
  SELECT
    * 
  FROM
  {% if var('demo_data_only', false) %} {{ ref('parta_diagnosis_code') }} {% else %} {{ source('medicare_cclf','cclf4_claim') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_TYPE_CD
    , CLM_PROD_TYPE_CD
    , {{ try_to_cast_int('CLM_VAL_SQNC_NUM') }}  as CLM_VAL_SQNC_NUM
    , CLM_DGNS_CD
    , BENE_EQTBL_BIC_HICN_NUM
    , PRVDR_OSCAR_NUM
    , {{ try_to_cast_date('CLM_FROM_DT') }} as CLM_FROM_DT
    , {{ try_to_cast_date('CLM_THRU_DT') }} as CLM_THRU_DT
    , CLM_POA_IND
    , DGNS_PRCDR_ICD_IND
    , CLM_BLG_PRVDR_OSCAR_NUM
    , FILE_GROUP_ID as FILE_NAME
    , null as FILE_DATE
from parta_diagnosis_code
