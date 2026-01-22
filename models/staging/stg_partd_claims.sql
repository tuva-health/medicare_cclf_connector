-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with partd_claims as (
  SELECT
    * 
  FROM
  {% if var('demo_data_only', false) %} {{ ref('partd_claims') }} {% else %} {{ source('medicare_cclf','partd_claims') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_LINE_NDC_CD
    , CLM_TYPE_CD
    , CLM_LINE_FROM_DT
    , PRVDR_SRVC_ID_QLFYR_CD
    , CLM_SRVC_PRVDR_GNRC_ID_NUM
    , CLM_DSPNSNG_STUS_CD
    , CLM_DAW_PROD_SLCTN_CD
    , CLM_LINE_SRVC_UNIT_QTY
    , CLM_LINE_DAYS_SUPLY_QTY
    , PRVDR_PRSBNG_ID_QLFYR_CD
    , CLM_PRSBNG_PRVDR_GNRC_ID_NUM
    , CLM_LINE_BENE_PMT_AMT
    , CLM_ADJSMT_TYPE_CD
    , CLM_EFCTV_DT
    , CLM_IDR_LD_DT
    , CLM_LINE_RX_SRVC_RFRNC_NUM
    , CLM_LINE_RX_FILL_NUM
    , CLM_PHRMCY_SRVC_TYPE_CD
    , FILE_NAME
    , FILE_DATE
from partd_claims