{{ config(materialized='table') }}

with encounter as(
  select distinct
    encounter_id
    ,cur_clm_uniq_id
  from {{ref('prof_claims_final')}}
)
select 
  
   coalesce(p.encounter_id, d.bene_mbi_id || replace(d.clm_thru_dt,'-','') || d.clm_pos_cd || d.clm_type_cd || d.payto_prvdr_npi_num) as encounter_id
  , d.bene_mbi_id || d.cur_clm_uniq_id || d.clm_line_num || d.clm_type_cd as encounter_detail_id
  , d.CUR_CLM_UNIQ_ID
  , d.CLM_LINE_NUM
  , d.BENE_MBI_ID
  , d.BENE_HIC_NUM
  , d.CLM_TYPE_CD
  , d.CLM_FROM_DT
  , d.CLM_THRU_DT
  , d.CLM_FED_TYPE_SRVC_CD
  , d.CLM_POS_CD
  , d.CLM_LINE_FROM_DT
  , d.CLM_LINE_THRU_DT
  , d.CLM_LINE_HCPCS_CD
  , d.CLM_LINE_CVRD_PD_AMT
  , d.CLM_PRMRY_PYR_CD
  , d.PAYTO_PRVDR_NPI_NUM
  , d.ORDRG_PRVDR_NPI_NUM
  , d.CLM_CARR_PMT_DNL_CD
  , d.CLM_PRCSG_IND_CD
  , d.CLM_ADJSMT_TYPE_CD
  , d.CLM_EFCTV_DT
  , d.CLM_IDR_LD_DT
  , d.CLM_CNTL_NUM
  , d.BENE_EQTBL_BIC_HICN_NUM
  , d.CLM_LINE_ALOWD_CHRG_AMT
  , d.CLM_DISP_CD
from {{ source('medicare_cclf','partb_dme')}} d
left join encounter p
on d.cur_clm_uniq_id = p.cur_clm_uniq_id
