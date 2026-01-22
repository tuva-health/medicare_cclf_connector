-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with partb_dme as (
  SELECT
    * 
  FROM
  {% if var('demo_data_only', false) %} {{ ref('partb_dme') }} {% else %} {{ source('medicare_cclf','partb_dme') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , CLM_LINE_NUM
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_TYPE_CD
    , CLM_FROM_DT
    , CLM_THRU_DT
    , CLM_FED_TYPE_SRVC_CD
    , CLM_POS_CD
    , CLM_LINE_FROM_DT
    , CLM_LINE_THRU_DT
    , CLM_LINE_HCPCS_CD
    , CLM_LINE_CVRD_PD_AMT
    , CLM_PRMRY_PYR_CD
    , PAYTO_PRVDR_NPI_NUM
    , ORDRG_PRVDR_NPI_NUM
    , CLM_CARR_PMT_DNL_CD
    , CLM_PRCSG_IND_CD
    , CLM_ADJSMT_TYPE_CD
    , CLM_EFCTV_DT
    , CLM_IDR_LD_DT
    , CLM_CNTL_NUM
    , BENE_EQTBL_BIC_HICN_NUM
    , CLM_LINE_ALOWD_CHRG_AMT
    , CLM_DISP_CD
    , CLM_BLG_PRVDR_NPI_NUM
    , CLM_RFRG_PRVDR_NPI_NUM
    , FILE_NAME
    , FILE_DATE
from partb_dme