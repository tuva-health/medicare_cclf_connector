-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with parta_claims_revenue_center_detail as (
  SELECT 
    * 
  FROM
  {% if var('demo_data_only', false) %} {{ ref('parta_claims_revenue_center_detail') }} {% else %} {{ source('medicare_cclf','cclf2_claim') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , CLM_LINE_NUM
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_TYPE_CD
    , {{ try_to_cast_date('CLM_LINE_FROM_DT') }} as CLM_LINE_FROM_DT
    , {{ try_to_cast_date('CLM_LINE_THRU_DT') }} as CLM_LINE_THRU_DT
    , CLM_LINE_PROD_REV_CTR_CD
    , {{ try_to_cast_date('CLM_LINE_INSTNL_REV_CTR_DT') }} as CLM_LINE_INSTNL_REV_CTR_DT
    , CLM_LINE_HCPCS_CD
    , BENE_EQTBL_BIC_HICN_NUM
    , PRVDR_OSCAR_NUM
    , {{ try_to_cast_date('CLM_FROM_DT') }} as CLM_FROM_DT
    , {{ try_to_cast_date('CLM_THRU_DT') }} as CLM_THRU_DT
    , {{ try_to_cast_numeric('CLM_LINE_SRVC_UNIT_QTY') }} as CLM_LINE_SRVC_UNIT_QTY
    , {{ try_to_cast_numeric('CLM_LINE_CVRD_PD_AMT') }} as CLM_LINE_CVRD_PD_AMT
    , HCPCS_1_MDFR_CD
    , HCPCS_2_MDFR_CD
    , HCPCS_3_MDFR_CD
    , HCPCS_4_MDFR_CD
    , HCPCS_5_MDFR_CD
    , CLM_REV_APC_HIPPS_CD
    , CLM_FAC_PRVDR_OSCAR_NUM
    , FILE_GROUP_ID as FILE_NAME
    , null as FILE_DATE
from parta_claims_revenue_center_detail
