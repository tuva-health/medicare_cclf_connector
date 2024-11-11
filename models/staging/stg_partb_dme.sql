select
      cur_clm_uniq_id
    , clm_line_num
    , bene_mbi_id
    , bene_hic_num
    , clm_type_cd
    , clm_from_dt
    , clm_thru_dt
    , clm_fed_type_srvc_cd
    , clm_pos_cd
    , clm_line_from_dt
    , clm_line_thru_dt
    , clm_line_hcpcs_cd
    , clm_line_cvrd_pd_amt
    , clm_prmry_pyr_cd
    , payto_prvdr_npi_num
    , ordrg_prvdr_npi_num
    , clm_carr_pmt_dnl_cd
    , clm_prcsg_ind_cd
    , clm_adjsmt_type_cd
    , clm_efctv_dt
    , clm_idr_ld_dt
    , clm_cntl_num
    , bene_eqtbl_bic_hicn_num
    , clm_line_alowd_chrg_amt
    , clm_disp_cd
    , file_name
    , file_date
from {{ source('medicare_cclf','partb_dme') }}