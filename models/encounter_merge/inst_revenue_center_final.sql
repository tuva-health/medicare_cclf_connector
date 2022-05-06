{{ config(materialized='table') }}

select
f.encounter_id
, r.bene_mbi_id || r.cur_clm_uniq_id || r.clm_line_num || r.CLM_LINE_PROD_REV_CTR_CD as encounter_detail_id
, r.CUR_CLM_UNIQ_ID
, r.CLM_LINE_NUM
, r.BENE_MBI_ID
, r.BENE_HIC_NUM
, r.CLM_TYPE_CD
, r.CLM_LINE_FROM_DT
, r.CLM_LINE_THRU_DT
, r.CLM_LINE_PROD_REV_CTR_CD
, r.CLM_LINE_INSTNL_REV_CTR_DT
, r.CLM_LINE_HCPCS_CD
, r.BENE_EQTBL_BIC_HICN_NUM
, r.PRVDR_OSCAR_NUM
, r.CLM_FROM_DT
, r.CLM_THRU_DT
, r.CLM_LINE_SRVC_UNIT_QTY
, r.CLM_LINE_CVRD_PD_AMT
, r.HCPCS_1_MDFR_CD
, r.HCPCS_2_MDFR_CD
, r.HCPCS_3_MDFR_CD
, r.HCPCS_4_MDFR_CD
, r.HCPCS_5_MDFR_CD
, r.CLM_REV_APC_HIPPS_CD
from {{ source('medicare_cclf','parta_claims_revenue_center_detail')}} r
inner join {{ ref('inst_claims_final')}} f
	on r.cur_clm_uniq_id = f.cur_clm_uniq_id