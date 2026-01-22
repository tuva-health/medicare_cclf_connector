with parta_claims_header as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('parta_claims_header') }} {% else %} {{ source('medicare_cclf','parta_claims_header') }}{% endif %}
)

select
      CUR_CLM_UNIQ_ID
    , PRVDR_OSCAR_NUM
    , BENE_MBI_ID
    , BENE_HIC_NUM
    , CLM_TYPE_CD
    , CLM_FROM_DT
    , CLM_THRU_DT
    , CLM_BILL_FAC_TYPE_CD
    , CLM_BILL_CLSFCTN_CD
    , PRNCPL_DGNS_CD
    , ADMTG_DGNS_CD
    , CLM_MDCR_NPMT_RSN_CD
    , CLM_PMT_AMT
    , CLM_NCH_PRMRY_PYR_CD
    , PRVDR_FAC_FIPS_ST_CD
    , BENE_PTNT_STUS_CD
    , DGNS_DRG_CD
    , CLM_OP_SRVC_TYPE_CD
    , FAC_PRVDR_NPI_NUM
    , OPRTG_PRVDR_NPI_NUM
    , ATNDG_PRVDR_NPI_NUM
    , OTHR_PRVDR_NPI_NUM
    , CLM_ADJSMT_TYPE_CD
    , CLM_EFCTV_DT
    , CLM_IDR_LD_DT
    , BENE_EQTBL_BIC_HICN_NUM
    , CLM_ADMSN_TYPE_CD
    , CLM_ADMSN_SRC_CD
    , CLM_BILL_FREQ_CD
    , CLM_QUERY_CD
    , DGNS_PRCDR_ICD_IND
    , CLM_MDCR_INSTNL_TOT_CHRG_AMT
    , CLM_MDCR_IP_PPS_CPTL_IME_AMT
    , CLM_OPRTNL_IME_AMT
    , CLM_MDCR_IP_PPS_DSPRPRTNT_AMT
    , CLM_HIPPS_UNCOMPD_CARE_AMT
    , CLM_OPRTNL_DSPRPRTNT_AMT
    , CLM_BLG_PRVDR_OSCAR_NUM
    , CLM_BLG_PRVDR_NPI_NUM
    , CLM_OPRTG_PRVDR_NPI_NUM
    , CLM_ATNDG_PRVDR_NPI_NUM
    , CLM_OTHR_PRVDR_NPI_NUM
    , CLM_CNTL_NUM
    , CLM_ORG_CNTL_NUM
    , CLM_CNTRCTR_NUM
    , FILE_NAME
    , FILE_DATE
from parta_claims_header
