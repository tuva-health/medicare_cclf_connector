-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with parta_claims_revenue_center_detail as (
  SELECT 
    * 
  FROM
  {% if var('demo_data_only', false) %} {{ ref('parta_claims_revenue_center_detail') }} {% else %} {{ source('medicare_cclf','parta_claims_revenue_center_detail') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , CLM_LINE_NUM
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_TYPE_CD
    , CLM_LINE_FROM_DT
    , CLM_LINE_THRU_DT
    , CLM_LINE_PROD_REV_CTR_CD
    , CLM_LINE_INSTNL_REV_CTR_DT
    , CLM_LINE_HCPCS_CD
    , BENE_EQTBL_BIC_HICN_NUM
    , PRVDR_OSCAR_NUM
    , CLM_FROM_DT
    , CLM_THRU_DT
    , CLM_LINE_SRVC_UNIT_QTY
    , CLM_LINE_CVRD_PD_AMT
    , HCPCS_1_MDFR_CD
    , HCPCS_2_MDFR_CD
    , HCPCS_3_MDFR_CD
    , HCPCS_4_MDFR_CD
    , HCPCS_5_MDFR_CD
    , CLM_REV_APC_HIPPS_CD
    , CLM_FAC_PRVDR_OSCAR_NUM
    , FILE_NAME
    , FILE_DATE
from parta_claims_revenue_center_detail