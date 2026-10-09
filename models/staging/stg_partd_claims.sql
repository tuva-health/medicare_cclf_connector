with partd_claims as (
  SELECT
    * 
  FROM
  {{ source('medicare_cclf','partd_claims') }}
)

select
      CUR_CLM_UNIQ_ID
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_LINE_NDC_CD
    , CLM_TYPE_CD
    , {{ try_to_cast_date('CLM_LINE_FROM_DT') }} as CLM_LINE_FROM_DT
    , PRVDR_SRVC_ID_QLFYR_CD
    , CLM_SRVC_PRVDR_GNRC_ID_NUM
    , CLM_DSPNSNG_STUS_CD
    , CLM_DAW_PROD_SLCTN_CD
    , {{ try_to_cast_numeric('CLM_LINE_SRVC_UNIT_QTY') }} as CLM_LINE_SRVC_UNIT_QTY
    , {{ try_to_cast_int('CLM_LINE_DAYS_SUPLY_QTY') }} as CLM_LINE_DAYS_SUPLY_QTY
    , PRVDR_PRSBNG_ID_QLFYR_CD
    , {{ try_to_cast_numeric('CLM_LINE_BENE_PMT_AMT') }} as CLM_LINE_BENE_PMT_AMT
    , CLM_ADJSMT_TYPE_CD
    , {{ try_to_cast_date('CLM_EFCTV_DT') }} as CLM_EFCTV_DT
    , {{ try_to_cast_date('CLM_IDR_LD_DT') }} as CLM_IDR_LD_DT
    , CLM_LINE_RX_SRVC_RFRNC_NUM
    , CLM_LINE_RX_FILL_NUM
    , CLM_PHRMCY_SRVC_TYPE_CD
    , CLM_PRSBNG_PRVDR_GNRC_ID_NUM
    , FILE_NAME
    , {{ try_to_cast_date('FILE_DATE') }} as FILE_DATE
from partd_claims
