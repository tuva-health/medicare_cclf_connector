{{ config(materialized='table') }}


select
  COALESCE(cc.encounter_id, l.encounter_id, u.encounter_id) as encounter_id
  ,u.CUR_CLM_UNIQ_ID
  ,u.PRVDR_OSCAR_NUM
  ,u.BENE_MBI_ID
  ,u.BENE_HIC_NUM
  ,u.CLM_TYPE_CD
  ,u.CLM_FROM_DT
  ,u.CLM_THRU_DT
  ,u.CLM_BILL_FAC_TYPE_CD
  ,u.CLM_BILL_CLSFCTN_CD
  ,u.PRNCPL_DGNS_CD
  ,u.ADMTG_DGNS_CD
  ,u.CLM_MDCR_NPMT_RSN_CD
  ,u.total_payment_amount
  ,u.CLM_NCH_PRMRY_PYR_CD
  ,u.PRVDR_FAC_FIPS_ST_CD
  ,u.BENE_PTNT_STUS_CD
  ,u.DGNS_DRG_CD
  ,u.CLM_OP_SRVC_TYPE_CD
  ,u.FAC_PRVDR_NPI_NUM
  ,u.OPRTG_PRVDR_NPI_NUM
  ,u.ATNDG_PRVDR_NPI_NUM
  ,u.OTHR_PRVDR_NPI_NUM
  ,u.CLM_ADJSMT_TYPE_CD
  ,u.CLM_EFCTV_DT
  ,u.CLM_IDR_LD_DT
  ,u.BENE_EQTBL_BIC_HICN_NUM
  ,u.CLM_ADMSN_TYPE_CD
  ,u.CLM_ADMSN_SRC_CD
  ,u.CLM_BILL_FREQ_CD
  ,u.CLM_QUERY_CD
  ,u.DGNS_PRCDR_ICD_IND
  ,u.CLM_MDCR_INSTNL_TOT_CHRG_AMT
  ,u.CLM_MDCR_IP_PPS_CPTL_IME_AMT
  ,u.CLM_OPRTNL_IME_AMT
  ,u.CLM_MDCR_IP_PPS_DSPRPRTNT_AMT
  ,u.CLM_HIPPS_UNCOMPD_CARE_AMT
  ,u.CLM_OPRTNL_DSPRTNT_AMT
from {{ ref('inst_claims_unique')}} u
left join {{ ref('inst_continuous_stay_crosswalk')}} cc
	on u.cur_clm_uniq_id = cc.cur_clm_uniq_id
left join {{ref('inst_overlap_crosswalk')}} l
	on u.cur_clm_uniq_id = l.cur_clm_uniq_id 
