{{ config(materialized='table') }}

  select
      bene_mbi_id || replace(clm_thru_dt,'-','') || clm_pos_cd || clm_type_cd || rndrg_prvdr_npi_num || clm_dgns_1_cd as encounter_id
      ,bene_mbi_id || cur_clm_uniq_id || clm_line_num || clm_type_cd as encounter_detail_id
      ,CUR_CLM_UNIQ_ID
      ,CLM_LINE_NUM
      ,BENE_MBI_ID
      ,BENE_HIC_NUM
      ,CLM_TYPE_CD
      ,CLM_FROM_DT
      ,CLM_THRU_DT
      ,RNDRG_PRVDR_TYPE_CD
      ,RNDRG_PRVDR_FIPS_ST_CD
      ,CLM_PRVDR_SPCLTY_CD
      ,CLM_FED_TYPE_SRVC_CD
      ,CLM_POS_CD
      ,CLM_LINE_FROM_DT
      ,CLM_LINE_THRU_DT
      ,CLM_LINE_HCPCS_CD
      ,CLM_LINE_CVRD_PD_AMT
      ,CLM_LINE_PRMRY_PYR_CD
      ,CLM_LINE_DGNS_CD
      ,CLM_RNDRG_PRVDR_TAX_NUM
      ,RNDRG_PRVDR_NPI_NUM
      ,CLM_CARR_PMT_DNL_CD
      ,CLM_PRCSG_IND_CD
      ,CLM_ADJSMT_TYPE_CD
      ,CLM_EFCTV_DT
      ,CLM_IDR_LD_DT
      ,CLM_CNTL_NUM
      ,BENE_EQTBL_BIC_HICN_NUM
      ,CLM_LINE_ALOWD_CHRG_AMT
      ,CLM_LINE_SRVC_UNIT_QTY
      ,HCPCS_1_MDFR_CD
      ,HCPCS_2_MDFR_CD
      ,HCPCS_3_MDFR_CD
      ,HCPCS_4_MDFR_CD
      ,HCPCS_5_MDFR_CD
      ,CLM_DISP_CD
      ,CLM_DGNS_1_CD
      ,CLM_DGNS_2_CD
      ,CLM_DGNS_3_CD
      ,CLM_DGNS_4_CD
      ,CLM_DGNS_5_CD
      ,CLM_DGNS_6_CD
      ,CLM_DGNS_7_CD
      ,CLM_DGNS_8_CD
      ,DGNS_PRCDR_ICD_IND
      ,CLM_DGNS_9_CD
      ,CLM_DGNS_10_CD
      ,CLM_DGNS_11_CD
      ,CLM_DGNS_12_CD
      ,HCPCS_BETOS_CD
  from {{ source('medicare_cclf','partb_physicians')}}
