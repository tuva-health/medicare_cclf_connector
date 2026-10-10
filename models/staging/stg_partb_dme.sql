with partb_dme as (
  SELECT
    * 
  FROM
  {{ source('medicare_cclf','partb_dme') }}
)

select
      cur_clm_uniq_id
    , clm_line_num
    , bene_mbi_id
    , bene_hic_num
    , clm_type_cd
    , {{ try_to_cast_date('clm_from_dt') }} as clm_from_dt
    , {{ try_to_cast_date('clm_thru_dt') }} as clm_thru_dt
    , clm_fed_type_srvc_cd
    , clm_pos_cd
    , {{ try_to_cast_date('clm_line_from_dt') }} as clm_line_from_dt
    , {{ try_to_cast_date('clm_line_thru_dt') }} as clm_line_thru_dt
    , clm_line_hcpcs_cd
    , {{ try_to_cast_numeric('clm_line_cvrd_pd_amt') }} as clm_line_cvrd_pd_amt
    , clm_prmry_pyr_cd
    , payto_prvdr_npi_num
    , ordrg_prvdr_npi_num
    , clm_carr_pmt_dnl_cd
    , clm_prcsg_ind_cd
    , clm_adjsmt_type_cd
    , {{ try_to_cast_date('clm_efctv_dt') }} as clm_efctv_dt
    , {{ try_to_cast_date('clm_idr_ld_dt') }} as clm_idr_ld_dt
    , clm_cntl_num
    , bene_eqtbl_bic_hicn_num
    , {{ try_to_cast_numeric('clm_line_alowd_chrg_amt') }} as clm_line_alowd_chrg_amt
    , clm_disp_cd
    , clm_blg_prvdr_npi_num
    , clm_rfrg_prvdr_npi_num
    , file_name
    , {{ try_to_cast_date('file_date') }} as file_date
from partb_dme
