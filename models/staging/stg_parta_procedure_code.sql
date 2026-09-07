-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with parta_procedure_code as (
  SELECT
    *
  FROM
  {% if var('demo_data_only', false) %} {{ ref('parta_procedure_code') }} {% else %} {{ source('medicare_cclf','cclf3_claim') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_TYPE_CD
    , cast({{ try_to_cast_int('CLM_VAL_SQNC_NUM') }} as {{ dbt.type_string() }}) as CLM_VAL_SQNC_NUM
    , CLM_PRCDR_CD
    , {{ try_to_cast_date('CLM_PRCDR_PRFRM_DT') }} as CLM_PRCDR_PRFRM_DT
    , BENE_EQTBL_BIC_HICN_NUM
    , PRVDR_OSCAR_NUM
    , {{ try_to_cast_date('CLM_FROM_DT') }} as CLM_FROM_DT
    , {{ try_to_cast_date('CLM_THRU_DT') }} as CLM_THRU_DT
    , DGNS_PRCDR_ICD_IND
    , CLM_BLG_PRVDR_OSCAR_NUM
    , FILE_GROUP_ID as FILE_NAME
    , null as FILE_DATE
from parta_procedure_code
