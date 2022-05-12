{{ config(materialized='table') }}

with encounter_header as(
select
      bene_mbi_id || replace(clm_thru_dt,'-','') || clm_pos_cd || clm_type_cd || ISNULL(rndrg_prvdr_npi_num,'') || clm_dgns_1_cd as encounter_id
      ,CUR_CLM_UNIQ_ID
from {{ source('medicare_cclf','partb_physicians')}}
where CLM_LINE_NUM = 1
)
,professional_prep as(
  select	  
  	    h.encounter_id
  	  , p.bene_mbi_id || p.cur_clm_uniq_id || p.clm_line_num || p.clm_type_cd as encounter_detail_id
	    , p.cur_clm_uniq_id
      , p.CLM_LINE_NUM
      , p.BENE_MBI_ID
      , p.BENE_HIC_NUM
      , p.CLM_TYPE_CD
      , p.CLM_FROM_DT
      , p.CLM_THRU_DT
      , p.RNDRG_PRVDR_TYPE_CD
      , p.RNDRG_PRVDR_FIPS_ST_CD
      , p.CLM_PRVDR_SPCLTY_CD
      , p.CLM_FED_TYPE_SRVC_CD
      , p.CLM_POS_CD
      , p.CLM_LINE_FROM_DT
      , p.CLM_LINE_THRU_DT
      , p.CLM_LINE_HCPCS_CD
      , cast(p.CLM_LINE_CVRD_PD_AMT as float) as CLM_LINE_CVRD_PD_AMT
      , p.CLM_LINE_PRMRY_PYR_CD
      , p.CLM_LINE_DGNS_CD
      , p.CLM_RNDRG_PRVDR_TAX_NUM
      , p.RNDRG_PRVDR_NPI_NUM
      , p.CLM_CARR_PMT_DNL_CD
      , p.CLM_PRCSG_IND_CD
      , p.CLM_ADJSMT_TYPE_CD
      , p.CLM_EFCTV_DT
      , p.CLM_IDR_LD_DT
      , p.CLM_CNTL_NUM
      , p.BENE_EQTBL_BIC_HICN_NUM
      , p.CLM_LINE_ALOWD_CHRG_AMT
      , p.CLM_LINE_SRVC_UNIT_QTY
      , p.HCPCS_1_MDFR_CD
      , p.HCPCS_2_MDFR_CD
      , p.HCPCS_3_MDFR_CD
      , p.HCPCS_4_MDFR_CD
      , p.HCPCS_5_MDFR_CD
      , p.CLM_DISP_CD
      , p.CLM_DGNS_1_CD
      , p.CLM_DGNS_2_CD
      , p.CLM_DGNS_3_CD
      , p.CLM_DGNS_4_CD
      , p.CLM_DGNS_5_CD
      , p.CLM_DGNS_6_CD
      , p.CLM_DGNS_7_CD
      , p.CLM_DGNS_8_CD
      , p.DGNS_PRCDR_ICD_IND
      , p.CLM_DGNS_9_CD
      , p.CLM_DGNS_10_CD
      , p.CLM_DGNS_11_CD
      , p.CLM_DGNS_12_CD
      , p.HCPCS_BETOS_CD
  from {{ source('medicare_cclf','partb_physicians')}} p
  inner join encounter_header h
  	on p.cur_clm_uniq_id = h.cur_clm_uniq_id
  )
  
  select * from professional_prep