{{ config(materialized='table') }}

select
  COALESCE(c.encounter_id, e.encounter_id) as encounter_id
  ,e.encounter_detail_id
  ,e.CUR_CLM_UNIQ_ID
  ,e.CLM_LINE_NUM
  ,e.BENE_MBI_ID
  ,e.BENE_HIC_NUM
  ,e.CLM_TYPE_CD
  ,e.CLM_FROM_DT
  ,e.CLM_THRU_DT
  ,e.RNDRG_PRVDR_TYPE_CD
  ,e.RNDRG_PRVDR_FIPS_ST_CD
  ,e.CLM_PRVDR_SPCLTY_CD
  ,e.CLM_FED_TYPE_SRVC_CD
  ,e.CLM_POS_CD
  ,e.CLM_LINE_FROM_DT
  ,e.CLM_LINE_THRU_DT
  ,e.CLM_LINE_HCPCS_CD
  ,e.CLM_LINE_CVRD_PD_AMT
  ,e.CLM_LINE_PRMRY_PYR_CD
  ,e.CLM_LINE_DGNS_CD
  ,e.CLM_RNDRG_PRVDR_TAX_NUM
  ,e.RNDRG_PRVDR_NPI_NUM
  ,e.CLM_CARR_PMT_DNL_CD
  ,e.CLM_PRCSG_IND_CD
  ,e.CLM_ADJSMT_TYPE_CD
  ,e.CLM_EFCTV_DT
  ,e.CLM_IDR_LD_DT
  ,e.CLM_CNTL_NUM
  ,e.BENE_EQTBL_BIC_HICN_NUM
  ,e.CLM_LINE_ALOWD_CHRG_AMT
  ,e.CLM_LINE_SRVC_UNIT_QTY
  ,e.HCPCS_1_MDFR_CD
  ,e.HCPCS_2_MDFR_CD
  ,e.HCPCS_3_MDFR_CD
  ,e.HCPCS_4_MDFR_CD
  ,e.HCPCS_5_MDFR_CD
  ,e.CLM_DISP_CD
  ,e.CLM_DGNS_1_CD
  ,e.CLM_DGNS_2_CD
  ,e.CLM_DGNS_3_CD
  ,e.CLM_DGNS_4_CD
  ,e.CLM_DGNS_5_CD
  ,e.CLM_DGNS_6_CD
  ,e.CLM_DGNS_7_CD
  ,e.CLM_DGNS_8_CD
  ,e.DGNS_PRCDR_ICD_IND
  ,e.CLM_DGNS_9_CD
  ,e.CLM_DGNS_10_CD
  ,e.CLM_DGNS_11_CD
  ,e.CLM_DGNS_12_CD
  ,e.HCPCS_BETOS_CD
from {{ ref('prof_claims_prep')}} e
left join {{ ref('prof_encounter_crosswalk_union')}} c
	on e.cur_clm_uniq_id = c.cur_clm_uniq_id
    and e.bene_mbi_id = c.bene_mbi_id
