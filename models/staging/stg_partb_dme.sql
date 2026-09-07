-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
select
      null as cur_clm_uniq_id
    , null as clm_line_num
    , null as bene_mbi_id
    , null as bene_hic_num
    , null as clm_type_cd
    , null as clm_from_dt
    , null as clm_thru_dt
    , null as clm_fed_type_srvc_cd
    , null as clm_pos_cd
    , null as clm_line_from_dt
    , null as clm_line_thru_dt
    , null as clm_line_hcpcs_cd
    , null as clm_line_cvrd_pd_amt
    , null as clm_prmry_pyr_cd
    , null as payto_prvdr_npi_num
    , null as ordrg_prvdr_npi_num
    , null as clm_carr_pmt_dnl_cd
    , null as clm_prcsg_ind_cd
    , null as clm_adjsmt_type_cd
    , null as clm_efctv_dt
    , null as clm_idr_ld_dt
    , null as clm_cntl_num
    , null as bene_eqtbl_bic_hicn_num
    , null as clm_line_alowd_chrg_amt
    , null as clm_disp_cd
    , null as clm_blg_prvdr_npi_num
    , null as clm_rfrg_prvdr_npi_num
    , null as FILE_NAME
    , null as FILE_DATE
