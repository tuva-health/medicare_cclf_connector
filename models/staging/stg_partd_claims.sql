select
      cur_clm_uniq_id
    , bene_mbi_id
    , bene_hic_num
    , clm_line_ndc_cd
    , clm_type_cd
    , clm_line_from_dt
    , prvdr_srvc_id_qlfyr_cd
    , clm_srvc_prvdr_gnrc_id_num
    , clm_dspnsng_stus_cd
    , clm_daw_prod_slctn_cd
    , clm_line_srvc_unit_qty
    , clm_line_days_suply_qty
    , prvdr_prsbng_id_qlfyr_cd
    , clm_prsbng_prvdr_gnrc_id_num
    , clm_line_bene_pmt_amt
    , clm_adjsmt_type_cd
    , clm_efctv_dt
    , clm_idr_ld_dt
    , clm_line_rx_srvc_rfrnc_num
    , clm_line_rx_fill_num
    , clm_phrmcy_srvc_type_cd
    , file_name
    , file_date
from {{ source('medicare_cclf','partd_claims') }}