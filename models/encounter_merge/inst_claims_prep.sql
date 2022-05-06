{{ config(materialized='table') }}


/**  Assign all instituional claims an encounter id  **/

select
   h.bene_mbi_id || h.clm_type_cd || isnull(h.prncpl_dgns_cd,'')  || isnull(h.fac_prvdr_npi_num,'')  || isnull(h.atndg_prvdr_npi_num,'') || isnull(r.clm_line_prod_rev_ctr_cd,'')  || replace(cast(h.clm_thru_dt as date),'-','') || replace(cast(ISNULL(h.clm_from_dt,'1900-01-01') as date),'-','') as encounter_id
  ,h.CUR_CLM_UNIQ_ID
  ,h.PRVDR_OSCAR_NUM
  ,h.BENE_MBI_ID
  ,h.BENE_HIC_NUM
  ,h.CLM_TYPE_CD
  ,cast(h.CLM_FROM_DT as date) as CLM_FROM_DT
  ,cast(h.CLM_THRU_DT as date) as CLM_THRU_DT
  ,h.CLM_BILL_FAC_TYPE_CD
  ,h.CLM_BILL_CLSFCTN_CD
  ,h.PRNCPL_DGNS_CD
  ,h.ADMTG_DGNS_CD
  ,h.CLM_MDCR_NPMT_RSN_CD
  ,a.total_payment_amount
  ,h.CLM_NCH_PRMRY_PYR_CD
  ,h.PRVDR_FAC_FIPS_ST_CD
  ,h.BENE_PTNT_STUS_CD
  ,h.DGNS_DRG_CD
  ,h.CLM_OP_SRVC_TYPE_CD
  ,h.FAC_PRVDR_NPI_NUM
  ,h.OPRTG_PRVDR_NPI_NUM
  ,h.ATNDG_PRVDR_NPI_NUM
  ,h.OTHR_PRVDR_NPI_NUM
  ,h.CLM_ADJSMT_TYPE_CD
  ,h.CLM_EFCTV_DT
  ,h.CLM_IDR_LD_DT
  ,h.BENE_EQTBL_BIC_HICN_NUM
  ,h.CLM_ADMSN_TYPE_CD
  ,h.CLM_ADMSN_SRC_CD
  ,h.CLM_BILL_FREQ_CD
  ,h.CLM_QUERY_CD
  ,h.DGNS_PRCDR_ICD_IND
  ,h.CLM_MDCR_INSTNL_TOT_CHRG_AMT
  ,h.CLM_MDCR_IP_PPS_CPTL_IME_AMT
  ,h.CLM_OPRTNL_IME_AMT
  ,h.CLM_MDCR_IP_PPS_DSPRPRTNT_AMT
  ,h.CLM_HIPPS_UNCOMPD_CARE_AMT
  ,h.CLM_OPRTNL_DSPRTNT_AMT
from {{source('medicare_cclf','parta_claims_header')}} h
inner join {{ ref('inst_payment_adjustment')}} a 
  on h.cur_clm_uniq_id = a.cur_clm_uniq_id
inner join {{source('medicare_cclf','parta_claims_revenue_center_detail')}} r
  on h.cur_clm_uniq_id = r.cur_clm_uniq_id
  and r.clm_line_num = 1